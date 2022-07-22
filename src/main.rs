use anyhow::{bail, Context};
use chrono::{Datelike, NaiveDateTime};
use clap::Parser;
use dropbox_sdk::{
    default_client::UserAuthDefaultClient,
    files::{self, DeleteArg, ListFolderArg},
};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    num::ParseIntError,
    path::PathBuf,
    str::FromStr,
    time::SystemTime, rc::Rc,
};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long)]
    dry_run: Option<bool>,

    #[clap(long)]
    read_from: ReadFrom,

    #[clap(long)]
    dbx_path: Option<String>,

    #[clap(long)]
    fs_path: Option<String>,
}

trait BackupReader {
    fn read(&self) -> Result<Vec<Backup>, BackupReadError>;
}

#[derive(thiserror::Error, Debug)]
#[error("backup read: {0}")]
struct BackupReadError(String);

impl From<String> for BackupReadError {
    fn from(s: String) -> Self {
        BackupReadError(s)
    }
}

struct DropboxBackupReader {
    client: Rc<UserAuthDefaultClient>,
    list_path: String,
}

impl BackupReader for DropboxBackupReader {
    fn read(&self) -> Result<Vec<Backup>, BackupReadError> {
        println!("Querying {} ...", self.list_path);
        let list_folder_result = files::list_folder(
            self.client.as_ref(),
            &ListFolderArg::new(self.list_path.to_string()),
        )
        .map_err(|o| format!("dbx read: {}", o))?
        .map_err(|o| format!("list: {}", o))?;
        println!("Querying {} ... done", self.list_path);
        if list_folder_result.has_more {
            // list_folder_result.cursor
            Err("need to handle more values with cursor!".to_string())?
        }
        let mut backups = Vec::with_capacity(list_folder_result.entries.len());
        for entry in list_folder_result.entries {
            if let files::Metadata::File(metadata) = entry {
                let backup_result: Result<Date, _> = metadata.name.parse();
                if let Ok(backup_date) = backup_result {
                    let backup = Backup {
                        name: metadata.name,
                        date: backup_date,
                    };
                    backups.push(backup);
                }
            }
        }
        Ok(backups)
    }
}

struct FileBackupReader {
    path: PathBuf,
}

impl BackupReader for FileBackupReader {
    fn read(&self) -> Result<Vec<Backup>, BackupReadError> {
        let f = File::open(&self.path).map_err(|o| format!("open: {}", o))?;
        let mut backups = Vec::new();
        let buf_reader = BufReader::new(f);
        for line_result in buf_reader.lines() {
            let line = line_result.map_err(|o| format!("read: {}", o))?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let date_result = line.parse::<Date>();
            if let Ok(date) = date_result {
                let backup = Backup {
                    date,
                    name: line.to_owned(),
                };
                backups.push(backup);
            }
        }
        Ok(backups)
    }
}

trait BackupDeleter {
    fn delete(&self, file: Removal) -> Result<(), BackupDeleteError>;
}

#[derive(thiserror::Error, Debug)]
#[error("backup delete: {0}")]
struct BackupDeleteError(String);

impl From<String> for BackupDeleteError {
    fn from(s: String) -> Self {
        BackupDeleteError(s)
    }
}

struct DropboxDeleter {
    client: Rc<UserAuthDefaultClient>,
}

impl BackupDeleter for DropboxDeleter {
    fn delete(&self, file: Removal) -> Result<(), BackupDeleteError> {
        println!("dbx delete: {} ...", &file.0);
        files::delete_v2(self.client.as_ref(), &DeleteArg::new(file.0))
            .map_err(|o| format!("dbx delete: {}", o))?
            .map_err(|o| format!("dbx delete arg: {}", o))?;
        println!("dbx delete: done");
        Ok(())
    }
}

struct NoopDeleter;

impl BackupDeleter for NoopDeleter {
    fn delete(&self, file: Removal) -> Result<(), BackupDeleteError> {
        println!("Noop remove: {}", &file.0);
        Ok(())
    }
}

#[derive(Debug)]
enum ReadFrom {
    Dropbox,
    Filesystem,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid read from")]
struct InvalidReadFrom;

impl FromStr for ReadFrom {
    type Err = InvalidReadFrom;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "dropbox" | "dbx" => Ok(ReadFrom::Dropbox),
            "filesystem" | "fs" => Ok(ReadFrom::Filesystem),
            _ => Err(InvalidReadFrom),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let backup_reader: Box<dyn BackupReader>;
    let backup_remover: Box<dyn BackupDeleter>;
    match args.read_from {
        ReadFrom::Dropbox => {
            let list_path = args.dbx_path.unwrap_or_default();
            if list_path.is_empty() {
                bail!("missing --dbx-path");
            }
            let auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
            let client = UserAuthDefaultClient::new(auth);
            let client = Rc::new(client);
            let dbx_reader = DropboxBackupReader {
                client: client.clone(),
                list_path,
            };
            let dbx_remover = DropboxDeleter{client};
            backup_reader = Box::new(dbx_reader);
            backup_remover = Box::new(dbx_remover);
        }
        ReadFrom::Filesystem => {
            let fs_path = args.fs_path.unwrap_or_default();
            if fs_path.is_empty() {
                bail!("missing --fs-path");
            }
            let file_reader = FileBackupReader {
                path: fs_path.into(),
            };
            backup_reader = Box::new(file_reader);
            backup_remover = Box::new(NoopDeleter);
        }
    };
    let mut backups = backup_reader.read().context("read backups")?;
    let mut years: Vec<Year> = Vec::new();
    for (i, backup) in backups.iter().enumerate() {
        let year = years.iter_mut().find(|o| o.num == backup.date.year);
        let year: &mut Year = if let Some(year) = year {
            year
        } else {
            let year = Year::new(backup.date.year);
            years.push(year);
            let idx = years.len() - 1;
            &mut years[idx]
        };
        let mut month = &mut year.months[(backup.date.month - 1) as usize];
        month.days[(backup.date.day - 1) as usize] = Some(Day::new(i as u32));
    }
    let seconds_since_epoch = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let seconds_since_epoch: i64 = seconds_since_epoch.as_secs().try_into().unwrap();
    let date_time = NaiveDateTime::from_timestamp(seconds_since_epoch, 0);
    let date = date_time.date();
    let cur = Date::new(date.year() as u32, date.month(), date.day());
    println!("today's date: {}/{}/{}", cur.year, cur.month, cur.day);
    let mut days_to_remove = Vec::new();
    for year in years.iter() {
        for (month_idx, month) in year.months.iter().enumerate() {
            let policy = policy_for(cur, year.num, month_idx as u32 + 1);
            let mut to_remove = apply_policy(policy, month);
            days_to_remove.append(&mut to_remove);
        }
    }
    let removals: Vec<Removal> = days_to_remove
        .into_iter()
        .map(|o| Removal(std::mem::take(&mut backups[o.idx as usize].name)))
        .collect();
    if args.dry_run.unwrap_or_default() {
        for removal in removals {
            println!("Dry run removing: {}", removal.0);
        }
        return Ok(());
    } else {
        for removal in removals {
            backup_remover.delete(removal).context("delete")?;
        }
    }
    Ok(())
}

struct Removal(String);

fn apply_policy(policy: MonthPolicy, month: &Month) -> Vec<Day> {
    match policy {
        MonthPolicy::Daily => Vec::new(),
        MonthPolicy::Weekly => keep_days(month, &[1, 8, 15, 22, 29]),
        MonthPolicy::BiMonthly => keep_days(month, &[1, 15]),
        MonthPolicy::First => keep_days(month, &[1]),
    }
}

fn keep_days(month: &Month, day_nums_to_keep: &[u32]) -> Vec<Day> {
    let mut result: Vec<Day> = Vec::with_capacity(31 - day_nums_to_keep.len());
    let mut day_nums_to_keep = day_nums_to_keep.iter();
    let mut next_day_to_keep = day_nums_to_keep.next();
    for (i, day) in month.days.iter().enumerate() {
        let num = (i + 1) as u32;
        if next_day_to_keep.is_none() {
            break;
        }
        let keep_day = *next_day_to_keep.unwrap();
        if keep_day == num {
            next_day_to_keep = day_nums_to_keep.next();
            continue;
        }
        if let Some(day) = day {
            result.push(*day);
        }
    }
    result
}

#[derive(Debug)]
enum MonthPolicy {
    Daily,
    Weekly,
    BiMonthly,
    First,
}

fn policy_for(cur: Date, year: u32, month: u32) -> MonthPolicy {
    if cur.year < year {
        return MonthPolicy::Daily;
    }
    // for previous years, adjust the month accordingly based on the year delta
    // to only have to consider the month delta
    let cur_month = if year < cur.year {
        cur.month + (12 * (cur.year - year))
    } else {
        cur.month
    };
    if cur_month <= month {
        return MonthPolicy::Daily;
    }
    if cur_month - month < 2 {
        return MonthPolicy::Daily;
    }
    if cur_month - month < 4 {
        return MonthPolicy::Weekly;
    }
    if cur_month - month < 9 {
        return MonthPolicy::BiMonthly;
    }
    if cur_month - month < 12 {
        return MonthPolicy::First;
    }
    MonthPolicy::First
}

#[derive(Debug, Clone, Copy)]
struct Date {
    year: u32,
    month: u32,
    day: u32,
}

impl Date {
    fn new(year: u32, month: u32, day: u32) -> Self {
        Self { year, month, day }
    }
}

#[derive(Debug, Clone)]
struct Backup {
    date: Date,
    name: String,
}

struct NotBackup;

impl FromStr for Date {
    type Err = NotBackup;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // yyyymmdd.tar.gz
        if s.len() < 11 {
            return Err(NotBackup);
        }
        let year: u32 = s[..4].parse()?;
        let month: u32 = s[4..6].parse()?;
        let day: u32 = s[6..8].parse()?;
        Ok(Date { year, month, day })
    }
}

impl From<ParseIntError> for NotBackup {
    fn from(_: ParseIntError) -> Self {
        NotBackup
    }
}

struct Year {
    num: u32,
    months: [Month; 12],
}

impl Year {
    fn new(num: u32) -> Self {
        Self {
            num,
            months: Default::default(),
        }
    }
}

#[derive(Default)]
struct Month {
    days: [Option<Day>; 31],
}

#[derive(Default, Clone, Copy)]
struct Day {
    idx: u32,
}

impl Day {
    fn new(idx: u32) -> Self {
        Self { idx }
    }
}

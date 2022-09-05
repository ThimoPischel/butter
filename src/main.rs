#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;
use std::env;

use anyhow::{bail, Context, Result};

use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt};
use tokio::process::Command;
use tokio::{fs, task};

use chrono::{NaiveDateTime, Utc};
use chronoutil::RelativeDuration;

use tempdir::TempDir;

#[derive(Deserialize)]
struct RcloneLsjsonItem {
    #[serde(rename = "Path")]
    path: String,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "MimeType")]
    mime_type: String,
    #[serde(rename = "ModTime")]
    mod_time: String,
    #[serde(rename = "IsDir")]
    is_dir: bool,
}
type RcloneLsjsonResult = Vec<RcloneLsjsonItem>;

type Arg = Option<Vec<Vec<String>>>;
#[derive(Serialize, Deserialize)]
struct Args {
    pub bevore_access_backups: Arg,
    pub after_access_backups: Arg,
    pub bevore_access_working: Arg,
    pub after_access_working: Arg,
}

#[derive(Deserialize, Clone)]
pub struct Job {
    pub name: String,
    pub typ: String,
    pub base_count: u32,
    pub inc_count: u32,
}

#[derive(Deserialize)]
struct ConfigItem {
    pub name: String,
    pub src: String,
    pub sub_dirs: Option<bool>,
    pub dests: Vec<String>,
    pub args: Option<Args>,
    pub jobs: Vec<Job>,
    pub ignore: Option<Vec<String>>,
}
type Config = Vec<ConfigItem>;

enum BackubJobType {
    Full,
    Inc { base: String },
}
struct BackupJob {
    src: String,
    dest: String,
    bu_type: BackubJobType,
    delete: Option<String>,
    ignore: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PathInfo {
    pub mod_time: String,
    pub is_dir: bool,
    pub sha1sum: Option<String>,
}
impl PathInfo {}

#[derive(Debug, Deserialize, Serialize)]
struct IncrementDelta {
    pub added: Vec<String>,
    pub deleted: Vec<String>,
    pub edited: Vec<String>,
}
impl IncrementDelta {}

#[derive(Debug, Serialize, Deserialize)]
struct BackupInfo {
    pub paths: HashMap<String, PathInfo>,
    pub increment_delta: Option<IncrementDelta>,
}
impl BackupInfo {
    pub async fn from_working_dir(path: &String, ignore: &Vec<String>) -> Result<BackupInfo> {
        let mut lsjson = Command::new("rclone");
        lsjson.arg("lsjson");
        lsjson.arg("-R");
        lsjson.arg(&path);

        for ig in ignore {
            lsjson.arg(format!("--exclude={}", ig));
        }

        let lsjson = lsjson
            .output()
            .await
            .with_context(|| format!("failed lsjson of: {path}"))?;

        let lsjson = lsjson
            .status
            .success()
            .then_some(lsjson.stdout)
            .with_context(|| format!("failed lsjson of: {path}"))?;

        let mut map: HashMap<String, PathInfo> =
            task::spawn_blocking(move || -> Result<HashMap<String, PathInfo>> {
                let lsjson = serde_json::from_slice::<RcloneLsjsonResult>(&lsjson)
                    .context("failed parsing lsjson result")?;

                let mut map = HashMap::new();

                for item in lsjson {
                    let path_info = PathInfo {
                        is_dir: item.is_dir,
                        sha1sum: None,
                        mod_time: item.mod_time,
                    };

                    map.insert(item.path, path_info);
                }

                Ok(map)
            })
            .await
            .with_context(|| format!("failed to spawn lsjson parsing for {path}"))?
            .with_context(|| format!("Could not create map for: {path}"))?;

        let mut sha1sums = Command::new("rclone");
        sha1sums.arg("sha1sum");
        sha1sums.arg(&path);

        for ig in ignore {
            sha1sums.arg(format!("--exclude={}", ig));
        }

        let sha1sums = sha1sums
            .output()
            .await
            .with_context(|| format!("Failed rclone sha1sum for: {path}"))?;

        let sha1sums = sha1sums
            .status
            .success()
            .then_some(sha1sums.stdout)
            .with_context(|| format!("Failed rclone sha1sum for: {path}"))?;

        map = task::spawn_blocking(move || -> Result<HashMap<String, PathInfo>> {
            let mut current = 0;
            let stop = sha1sums.len();
            let sha_length = 40;
            let spacing = 2;

            while stop - current > sha_length + spacing + 1 {
                let sha = String::from_utf8(sha1sums[current..current + sha_length].to_vec())
                    .context("failed sha1sum slice to string")?;

                current = current + sha_length + spacing;

                let mut eop = current + 1;

                while sha1sums[eop] != b'\n' {
                    eop += 1;
                }

                let file = String::from_utf8(sha1sums[current..eop].to_vec())
                    .context("failed sha1sum slice to string")?;

                current = eop + 1;

                map.get_mut(&file)
                    .context("got a sha1sum for not existing file lol")?
                    .sha1sum = Some(sha);
            }

            Ok(map)
        })
        .await
        .with_context(|| format!("failed to spawn sha1sums parsing for {path}"))?
        .with_context(|| format!("failed adding sha1sums to: {path}"))?;

        Ok(BackupInfo {
            paths: map,
            increment_delta: None,
        })
    }

    pub async fn from_file(path: &String) -> Result<BackupInfo> {
        let cat = Command::new("rclone")
            .arg("cat")
            .arg(path)
            .output()
            .await
            .with_context(|| format!("failed awaiting cat of: {path}"))?;

        if !cat.status.success() {
            bail!("failed reading file: {path}");
        }

        let backup_info = serde_json::from_slice::<BackupInfo>(&cat.stdout)
            .with_context(|| format!("failed reading file: {path}"))?;

        Ok(backup_info)
    }

    pub async fn calc_increment(mut self, base: BackupInfo) -> (BackupInfo, BackupInfo) {
        task::spawn_blocking(move || -> (BackupInfo, BackupInfo) {
            let mut deleted: Vec<String> = Vec::new();
            let mut added: Vec<String> = Vec::new();
            let mut edited: Vec<String> = Vec::new();

            for (key, value) in &self.paths {
                let path_info = match base.paths.get(key) {
                    Some(path_info) => path_info,
                    None => {
                        added.push(key.clone());
                        continue;
                    }
                };

                if path_info.is_dir != value.is_dir {
                    deleted.push(key.clone());
                    added.push(key.clone());
                } else if path_info.mod_time == value.mod_time && path_info.sha1sum == value.sha1sum
                {
                    continue;
                } else if !path_info.is_dir {
                    edited.push(key.clone());
                }
            }

            for key in base.paths.keys() {
                if !self.paths.contains_key(key) {
                    deleted.push(key.clone());
                }
            }

            self.increment_delta = Some(IncrementDelta {
                deleted,
                added,
                edited,
            });

            (self, base)
        })
        .await
        .expect("failed to spawn calc_increment future")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = read_config().await.context("failed to read config")?;
    let config = match config {
        Some(config) => config,
        None => {
            println!("No Config found");
            println!("Create one in ~/Config/butter/config.json");
            return Ok(());
        }
    };

    let mut selected: Option<Vec<String>> = None;

    {
        let args: Vec<String> = env::args().collect();

        if args.len() > 1 {
            selected = Some(args[1..].to_vec());
        }
    }

    let mut handles = vec![];

    for config_job in config {
        if let Some(selected) = &selected {
            if selected.contains(&config_job.name) {
                continue;
            }
        }

        handles.push(tokio::spawn(backup_job_task(config_job)));
    }

    for handle in handles {
        handle
            .await
            .context("failed to join task handle")?
            .context("job failed")?;
    }

    Ok(())
}

async fn read_config() -> Result<Option<Config>> {
    let config = match fs::read("/home/thimo/Config/butter/config.json").await {
        Ok(config) => config,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => bail!(err),
    };

    let config = serde_json::from_slice::<Config>(&config).context("Failed parsing config")?;

    Ok(Some(config))
}

async fn backup_job_task(backup_job: ConfigItem) -> Result<()> {
    if let Some(args) = &backup_job.args {
        run_command_list(&args.bevore_access_backups).await?;
    }

    let mut jobs: Vec<BackupJob> = vec![];

    if backup_job.sub_dirs != Some(true) {
        //fill jobs Normal
        let mut handles = vec![];

        for dest in &backup_job.dests {
            for job in &backup_job.jobs {
                println!("generating jobs for {} - {}", &backup_job.name, &job.name);
                //TODO: less then optimal
                let i_src = backup_job.src.clone();
                let i_dest = path_join(&[dest, &job.name]);
                let i_job = job.clone();
                let i_ignore: Vec<String> = match &backup_job.ignore {
                    None => vec![],
                    Some(o) => o.clone(),
                };

                handles.push(tokio::spawn(async move {
                    create_job(i_src, i_dest, i_job, i_ignore).await
                }));
            }
        }

        for handle in handles {
            match handle.await {
                Ok(Ok(Some(job))) => jobs.push(job),
                Ok(Ok(None)) => {}
                Err(err) => {
                    println!("Failed to spawn job: {}", backup_job.name);
                    println!("Error: {err:?}");
                    continue;
                }
                Ok(Err(err)) => {
                    println!("Failed one Job at: {}", backup_job.name);
                    println!("Error: {err:?}");
                    continue;
                }
            }
        }

        if jobs.is_empty() {
            //nothing to do
            if let Some(args) = &backup_job.args {
                run_command_list(&args.after_access_backups).await?;
            }

            return Ok(());
        }
    }

    if let Some(args) = &backup_job.args {
        run_command_list(&args.bevore_access_working).await?;
    }

    if backup_job.sub_dirs == Some(true) {
        // fill jobs Subdirs
        let sub_dirs = create_dir_and_get_dir_names(&backup_job.src)
            .await
            .with_context(|| format!("couldnt access working dir: {}", &backup_job.src))?;

        let mut handles = vec![];

        for sub_dir in sub_dirs {
            for dest in &backup_job.dests {
                for job in &backup_job.jobs {
                    println!(
                        "generating jobs for {} - {} | {}",
                        &backup_job.name, &job.name, &sub_dir
                    );

                    //TODO: less then optimal
                    let i_src = path_join(&[&backup_job.src, &sub_dir]);
                    let i_dest = path_join(&[dest, &sub_dir, &job.name]);
                    let i_job = job.clone();
                    let i_ignore: Vec<String> = match &backup_job.ignore {
                        None => vec![],
                        Some(o) => o.clone(),
                    };

                    handles.push(tokio::spawn(async move {
                        create_job(i_src, i_dest, i_job, i_ignore).await
                    }));
                }
            }
        }

        for handle in handles {
            match handle.await {
                Ok(Ok(Some(job))) => jobs.push(job),
                Ok(Ok(None)) => {}
                Err(err) => {
                    println!("Failed to spawn job: {}", backup_job.name);
                    println!("Error: {err:?}");
                    continue;
                }
                Ok(Err(err)) => {
                    println!("Failed one Job at: {}", backup_job.name);
                    println!("Error: {err:?}");
                    continue;
                }
            }
        }
    }

    {
        //spawn jobs
        let mut handles = vec![];

        for job in jobs {
            println!("Fire up job {} -> {}", &job.src, &job.dest);

            handles.push(tokio::spawn(run_job(job)));
        }

        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {},
                Err(err) => {
                    println!("Failed to spawn job: {}", backup_job.name);
                    println!("Error: {err:?}");
                    continue;
                }
                Ok(Err(err)) => {
                    println!("Failed one Job at: {}", backup_job.name);
                    println!("Error: {err:?}");
                    continue;
                }
            }
        }
    }

    if let Some(args) = &backup_job.args {
        run_command_list(&args.after_access_working).await?
    }

    if let Some(args) = &backup_job.args {
        run_command_list(&args.after_access_backups).await?
    }

    Ok(())
}

async fn run_job(job: BackupJob) -> Result<()> {
    if let Some(path) = &job.delete {
        //delete old
        println!("Cleanup: {}", path);

        Command::new("rclone")
            .arg("purge")
            .arg(&path)
            .spawn()
            .with_context(|| format!("Failed cleanup: {path}"))?
            .wait()
            .await
            .with_context(|| format!("Failed cleanup: {path}"))?;
    }

    let i_src = job.src.clone();
    let i_ignore = job.ignore.clone();
    let backup = tokio::spawn(async move { BackupInfo::from_working_dir(&i_src, &i_ignore).await });

    let root = TempDir::new("butter_tmp").context("couldnt create tmp dir")?;
    let backup_file_path = root.path().join("backup.json");

    let mut backup: BackupInfo = backup
        .await
        .context("failed to spawn backup future")?
        .context("couldnt create backup")?;

    if let BackubJobType::Inc { base } = &job.bu_type {
        let base_text = Command::new("rclone")
            .arg("cat")
            .arg(&base)
            .output()
            .await
            .with_context(|| format!("couldnt read file: {base}"))?;
        let base_text = base_text
            .status
            .success()
            .then_some(base_text.stdout)
            .with_context(|| format!("couldnt read file: {base}"))?;

        let base: BackupInfo = serde_json::from_slice(&base_text)
            .with_context(|| format!("couldnt parse file: {base}"))?;

        (backup, _) = backup.calc_increment(base).await;
    }

    {
        // writing backupinfo to file
        let mut backup_file = File::create(&backup_file_path)
            .await
            .context("couldnt create tmp file")?;

        let info = serde_json::to_vec_pretty(&backup).context("failed serializing backupinfo")?;

        println!("{}", String::from_utf8_lossy(&info));

        backup_file
            .write_all(&info)
            .await
            .context("failed writing to tmp file")?;
    }

    let mut file_task = Command::new("rclone")
        .arg("copyto")
        .arg(&backup_file_path)
        .arg(job.dest.clone() + ".json")
        .spawn()
        .context("failed spawning copy task for backup info")?;

    match &job.bu_type {
        BackubJobType::Full => {
            let mut src = Command::new("rclone");

            src.arg("copy");
            src.arg(&job.src);
            src.arg(&job.dest);

            for ig in job.ignore {
                src.arg(format!("--exclude={}", ig));
            }

            src.spawn()
                .context("failed spawning copy task for working dir")?
                .wait()
                .await
                .with_context(|| format!("failed copying src to {}", job.dest))?;
        }
        BackubJobType::Inc { base } => {
            let mut copy_handles = vec![];

            let increment_delta = backup
                .increment_delta
                .as_ref()
                .context("backup job increment without calculated increment")?;
            let added = &increment_delta.added;
            let edited = &increment_delta.edited;

            copy_to_list(&job.src, &job.dest, added, &mut copy_handles).await?;
            copy_to_list(&job.src, &job.dest, edited, &mut copy_handles).await?;

            for mut copy_handle in copy_handles {
                if let Err(e) = copy_handle.wait().await {
                    println!("Err while increment copies: {}", e);
                }
            }
        }
    }

    file_task
        .wait()
        .await
        .with_context(|| format!("failed copying backupinfo to {}.json", job.dest))?;

    Ok(())
}

async fn copy_to_list(
    src_root: &str,
    dest_root: &str,
    copy: &[String],
    handles: &mut Vec<tokio::process::Child>,
) -> Result<()> {
    for path in copy {
        let handle = Command::new("rclone")
            .arg("copyto")
            .arg(path_join(&[src_root, path]))
            .arg(path_join(&[dest_root, path]))
            .spawn()
            .with_context(|| {
                format!("!Failed spawning copy for {}", path_join(&[src_root, path]))
            })?;

        handles.push(handle);
    }

    Ok(())
}

async fn create_job(
    src: String,
    dest: String,
    backup_job: Job,
    ignore: Vec<String>,
) -> Result<Option<BackupJob>> {
    let now = Utc::now().naive_utc();
    let now_str = now.format("%Y%m%d:%H%M%S").to_string();
    let now_date = now.date();

    let dir_names = create_dir_and_get_dir_names(&dest).await?;

    if dir_names.is_empty() {
        return Ok(Some(BackupJob {
            src: src.clone(),
            dest: path_join(&[&dest, &now_str, &now_str]),
            bu_type: BackubJobType::Full,
            delete: None,
            ignore,
        }));
    }

    let last_collection_name = &dir_names[dir_names.len() - 1];
    let last_collection_path = path_join(&[&dest, last_collection_name]);
    let last_collection = create_dir_and_get_dir_names(&last_collection_path).await?;
    let last_backup_name = &last_collection[last_collection.len() - 1];

    let last_backup_time = NaiveDateTime::parse_from_str(last_backup_name, "%Y%m%d:%H%M%S")
        .with_context(|| {
            format!(
                "corrupted backup dir (could not parse time) => {} / {} / {}",
                &dest, &last_collection_name, &last_backup_name
            )
        })?
        .date();

    match backup_job.typ.as_str() {
        "all" => {}
        "day" => {
            if last_backup_time + RelativeDuration::days(1) > now_date {
                return Ok(None);
            }
        }
        "week" => {
            if last_backup_time + RelativeDuration::weeks(1) > now_date {
                return Ok(None);
            }
        }
        "month" => {
            if last_backup_time + RelativeDuration::months(1) > now_date {
                return Ok(None);
            }
        }
        "year" => {
            if last_backup_time + RelativeDuration::years(1) > now_date {
                return Ok(None);
            }
        }
        _ => bail!("unknown backup type: {}", &backup_job.typ),
    }

    if last_collection.is_empty() {
        bail!(
            "corrupted backup dir (found collection without base) => {} / {}",
            &dest,
            &last_collection_name
        );
    }

    let job = if last_collection.len() - 1 < backup_job.inc_count as usize {
        //increment
        BackupJob {
            src: src.clone(),
            dest: path_join(&[&dest, last_collection_name, &now_str]),
            bu_type: BackubJobType::Inc {
                base: path_join(&[&dest, last_collection_name, last_backup_name]) + ".json",
            },
            delete: None,
            ignore,
        }
    } else if dir_names.len() <= backup_job.base_count as usize {
        //full
        BackupJob {
            src: src.clone(),
            dest: path_join(&[&dest, &now_str, &now_str]),
            bu_type: BackubJobType::Full,
            delete: None,
            ignore,
        }
    } else {
        BackupJob {
            src: src.clone(),
            dest: path_join(&[&dest, &now_str, &now_str]),
            bu_type: BackubJobType::Full,
            delete: Some(path_join(&[&dest, &dir_names[0]])),
            ignore,
        }
    };

    Ok(Some(job))
}

async fn create_dir_and_get_dir_names(root: &String) -> Result<Vec<String>> {
    Command::new("rclone")
        .arg("mkdir")
        .arg(&root)
        .spawn()
        .with_context(|| format!("could not make dir: {root}"))?
        .wait()
        .await
        .with_context(|| format!("could not make dir: {root}"))?;

    let dirs = Command::new("rclone")
        .arg("lsjson")
        .arg(&root)
        .output()
        .await
        .with_context(|| format!("failed reading dirs from: {root}"))?;
    let dirs = dirs
        .status
        .success()
        .then_some(dirs.stdout)
        .with_context(|| format!("failed reading dirs from: {root}"))?;

    let dirs: RcloneLsjsonResult =
        serde_json::from_slice(&dirs).with_context(|| format!("Failed parsing lsjson: {root}"))?;

    let mut dir_names = vec![];

    for dir in dirs {
        if dir.is_dir {
            dir_names.push(dir.name);
        }
    }

    dir_names.sort();

    Ok(dir_names)
}

async fn run_command_list(args: &Arg) -> Result<()> {
    let cmds = match &args {
        Some(cmds) => cmds,
        None => return Ok(()),
    };

    for cmd in cmds {
        if cmd.is_empty() {
            continue;
        }

        let mut command = Command::new(&cmd[0]);

        for c_arg in &cmd[1..] {
            command.arg(&c_arg);
        }

        command
            .spawn()
            .with_context(|| format!("failed running: {cmd:?}"))?
            .wait()
            .await
            .with_context(|| format!("failed running: {cmd:?}"))?;
    }

    Ok(())
}

fn path_join(paths: &[&str]) -> String {
    if env::consts::OS == "windows" {
        paths.join("\\")
    } else {
        paths.join("/")
    }
}

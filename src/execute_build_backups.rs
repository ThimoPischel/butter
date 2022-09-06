
use anyhow::{bail, Context, Result};

use tokio::fs::File;
use tokio::io:: AsyncWriteExt;
use tokio::process::Command;


use chrono::{NaiveDateTime, Utc};
use chronoutil::RelativeDuration;

use tempdir::TempDir;

use super::config::*;
use super::helper::*;
use super::backup_info::*;

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

pub async fn execute_build_backups(args: Vec<String>) -> Result<()> {
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
    if args.len() > 1 {
        selected = Some(args[1..].to_vec());
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
    //delete old
    if let Some(path) = &job.delete {
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
        let base = BackupInfo::from_file(&base).await?;
        (backup, _) = backup.calc_increment(base).await;
    }

    // writing backupinfo to file
    {
        let mut backup_file = File::create(&backup_file_path)
            .await
            .context("couldnt create tmp file")?;

        let info = serde_json::to_vec_pretty(&backup).context("failed serializing backupinfo")?;

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
        BackubJobType::Inc { base: _ } => {
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

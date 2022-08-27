#[macro_use]
extern crate serde_derive;
extern crate tempdir;

use tokio::time::{sleep, Duration};
use tokio::{ fs, task };
use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt};
use tokio::process::Command;

use serde::{Deserialize, Serialize};

use chrono::{ NaiveDateTime, Utc };
use chronoutil::RelativeDuration;

use std::collections::HashMap;
use std::process::Stdio;
use std::env;
use std::path::Path;

use tempdir::TempDir;



#[derive(Deserialize)]
struct RcloneLsjsonItem {
    #[serde(rename = "Path")] 
    path:       String,
    #[serde(rename = "Name")] 
    name:       String,
    #[serde(rename = "Size")] 
    size:       i64,
    #[serde(rename = "MimeType")] 
    mime_type:  String,
    #[serde(rename = "ModTime")] 
    mod_time:   String,
    #[serde(rename = "IsDir")] 
    is_dir:     bool
}
type RcloneLsjsonResult = Vec<RcloneLsjsonItem>;

type Arg = Option<Vec<Vec<String>>>;
#[derive(Serialize, Deserialize)]
struct Args {
    pub bevore_access_backups:  Arg,
    pub after_access_backups:   Arg,
    pub bevore_access_working:  Arg,
    pub after_access_working:   Arg,
}

#[derive(Deserialize, Clone)]
pub struct Job{
    pub name: String,
    pub typ: String,
    pub base_count: u32,
    pub inc_count: u32
}

#[derive(Deserialize)]
struct ConfigItem {
    pub name:       String,
    pub src:        String,
    pub sub_dirs:   Option<bool>,
    pub dests:      Vec<String>,
    pub args:       Option<Args>,
    pub jobs:       Vec<Job>,
    pub ignore:    Option<Vec<String>>
}
type Config = Vec<ConfigItem>;


enum BackubJobType{
    Full,
    Inc { base: String }
}
struct BackupJob {
    src: String,
    dest: String,
    bu_type: BackubJobType,
    delete: Option<String>,
    ignore: Vec<String>
}

#[derive(Debug, Deserialize, Serialize)]
struct PathInfo {
    pub mod_time:   String,
    pub is_dir:     bool,
    pub sha1sum:    Option<String>
} impl PathInfo {

}

#[derive(Debug, Deserialize, Serialize)]
struct IncrementDelta {
    pub added:      Vec<String>,
    pub deleted:    Vec<String>,
    pub edited:     Vec<String>
} impl IncrementDelta {

}

#[derive(Debug, Serialize, Deserialize)]
struct BackupInfo {
    pub paths: HashMap<String, PathInfo>,
    pub increment_delta: Option<IncrementDelta>
} impl BackupInfo {
    pub async fn from_working_dir(path: &String, ignore: &Vec<String>) -> Result<BackupInfo, ()> {
        let mut lsjson = Command::new("rclone");
        lsjson.arg("lsjson");
        lsjson.arg("-R");
        lsjson.arg(&path);
        for ig in ignore {
            lsjson.arg(format!("--exclude={}", ig));
        }
        let lsjson = lsjson.output();
            
            let lsjson = match lsjson.await {
                Ok(o) => {
                    if o.status.success() {
                        o.stdout
                } else {
                    println!("failed lsjson of: {}", &path);
                    return Err(());
                }
            },
            Err(e) => {
                println!("failed lsjson of: {}", &path);
                return Err(());
            }
        };
        
        let mut map : HashMap<String, PathInfo> = match task::spawn_blocking(move || -> Result<HashMap<String, PathInfo>, ()> {
            let lsjson : RcloneLsjsonResult = match serde_json::from_slice(&lsjson) {
                Ok(o) => o,
                Err(e) => {
                    println!("failed parsing lsjson result: {}", e);
                    return Err(());
                }
            };

            let mut map = HashMap::new();
            for item in lsjson {
                map.insert(item.path, PathInfo{
                    is_dir: item.is_dir,
                    sha1sum: None,
                    mod_time: item.mod_time
                }
            );
            }
            Ok(map)
        }).await {
            Ok(Ok(o)) => {
                o
            },
            _ => {
                println!("Could not create map for: {}", &path);
                return Err(());
            }
        };

        let mut sha1sums = Command::new("rclone");
        sha1sums.arg("sha1sum");
        sha1sums.arg(&path);
        for ig in ignore {
            sha1sums.arg(format!("--exclude={}", ig));
        }
        let sha1sums = match sha1sums.output().await{
            Ok(o) => {
                if o.status.success() {
                    o.stdout
                } else {
                    println!("Failed rclone sha1sum for: {}", &path );
                    return Err(());
                }
            },
            _ => {
                println!("Failed rclone sha1sum for: {}", &path );
                return Err(());
            }
        };
        
        let sha_task = task::spawn_blocking( move || -> Result<HashMap<String, PathInfo>, ()> {
            let mut current = 0;
            let stop = sha1sums.len();
            let sha_length = 40;
            let spacing = 2;
            while stop - current > sha_length + spacing + 1 {
                let sha = String::from_utf8(sha1sums[current..current+sha_length].to_vec()).expect("failed sha1sum slice to string");
                current = current + sha_length + spacing;
                let mut eop = current + 1;
                while sha1sums[eop] != '\n' as u8 {
                    eop+=1;
                }
                let file = String::from_utf8(sha1sums[current..eop].to_vec()).expect("failed sha1sum slice to string");
                current = eop + 1;
                map.get_mut(&file).expect("got a sha1sum for not existing file lol").sha1sum = Some(sha);
            }
            Ok(map)
        }).await;
        
        map = match sha_task {
            Ok(Ok(o)) => {
                o
            }
            _ => {
                println!("failed adding sha1sums to: {}", &path);
                return Err(());
            }
        };

        
        Ok(BackupInfo{
            paths: map,
            increment_delta: None
        })
    }
    pub async fn from_file(path: &String) -> Result<BackupInfo, ()> {
        let cat = Command::new("rclone")
            .arg("cat")
            .arg(path)
            .output();

        let cat = match cat.await {
            Ok(o) => o,
            _ => {
                println!("failed awaiting cat of: {}", path);
                return Err(());
            }
        };

        if !cat.status.success() {
            println!("failed reading file: {}", path);
            return Err(());
        }

        match serde_json::from_slice(&cat.stdout) {
            Ok(o) => Ok(o),
            _ => {
                println!("failed reading file: {}", path);
                return Err(());
            }
        }        
    }
    pub async fn calc_increment(mut self, base: BackupInfo) -> (BackupInfo, BackupInfo) {
        task::spawn_blocking(move || -> (BackupInfo, BackupInfo) {            
            let mut deleted : Vec<String> = Vec::new();
            let mut added : Vec<String> = Vec::new();
            let mut edited : Vec<String> = Vec::new();
    
            for (k, v) in &self.paths {
                match base.paths.get(k) {
                    Some(o) => {
                        if o.is_dir != v.is_dir {
                            deleted.push(k.clone());
                            added.push(k.clone());
                        } else if o.mod_time == v.mod_time && o.sha1sum == v.sha1sum {
                            continue;
                        } else if !o.is_dir {
                            edited.push(k.clone());
                        }
                    },
                    None => {
                        added.push(k.clone());
                    }
                }
            }
            for (k,_) in &base.paths {
                if !self.paths.contains_key(k){
                    deleted.push(k.clone());
                }
            }
            self.increment_delta = Some(IncrementDelta{
                deleted: deleted,
                added: added,
                edited: edited
            });
            (self, base)
        }).await.unwrap()       
    }
}


#[tokio::main]
async fn main() {
    let config = match read_config().await {
        Ok(o) => o,
        Err(_) => {
            println!("! no config !");
            return;
        }
    };
    let mut selected : Option<Vec<String>> = None;
    {
        let args: Vec<String> = env::args().collect();
        if args.len() > 1 {
            selected = Some(args[1..].to_vec());
        }
    }
    
    let mut handle = vec![];
    for config_job in config {
        match &selected {
            Some(o) => {
                if ! o.contains(&config_job.name) {
                    continue;
                }
            },
            _ => {}
        }

        handle.push(tokio::spawn( async move {
            backup_job_task(&config_job).await
        }));
    }
    
    for h in handle {
        _ = h.await.unwrap();
    }
}



async fn read_config() -> Result<Config ,()> {
    let config_text = match fs::read("/home/thimo/Config/butter/config.json").await {
        Ok(c) => c,
        Err(e) => {
            println!("No Config found");
            println!("Create one in ~/Config/butter/config.json");
            return Err(());
        }
    };
    match serde_json::from_slice(&config_text) {
        Ok(c) => Ok(c),
        Err(e) => {
            println!("Failed parsing config {}", e);
            return Err(());
        }
    }    
}

async fn backup_job_task(backup_job: &ConfigItem) -> Result<(),()> {

    match &backup_job.args {
        Some(args) =>  match &run_command_list(&args.bevore_access_backups).await {
            Ok(_) => {},
            _  => return Err(())
        },
        _ => {}
    }
    

    let mut jobs : Vec<BackupJob> = vec![];
    if backup_job.sub_dirs != Some(true) { //fill jobs Normal
        let mut handles = vec![];
        for dest in &backup_job.dests {
            for job in &backup_job.jobs {
                println!("generating jobs for {} - {}", &backup_job.name, &job.name);
                //TODO: less then optimal
                let i_src = backup_job.src.clone();
                let i_dest = path_join(&[ dest.clone(), job.name.clone() ]);
                let i_job = job.clone();
                let i_ignore : Vec<String> = match &backup_job.ignore {
                    None => vec![],
                    Some(o) => o.clone() 
                };
                handles.push(tokio::spawn(async move {
                    create_job(i_src, i_dest, i_job, i_ignore).await
                }));
            }
        }   

        for handle in handles{
            match handle.await {
                Ok(Some(o)) => jobs.push(o),
                _ => {
                    println!("Failed one Job at: {}", &backup_job.name);
                    continue;
                }
            }
        }

        if jobs.len() == 0 { //nothing to do
            match &backup_job.args { //after backup
                Some(args) =>  match &run_command_list(&args.after_access_backups).await {
                    Ok(_) => {},
                    _  => return Err(())
                },
                _ => {}
            }
            return Ok(())      
        }
    }

    match &backup_job.args { //bevor working
        Some(args) =>  match &run_command_list(&args.bevore_access_working).await {
            Ok(_) => {},
            _  => return Err(())
        },
        _ => {}
    }

    if backup_job.sub_dirs == Some(true) { // fill jobs Subdirs
        let sub_dirs = match create_dir_and_get_dir_names(&backup_job.src).await {
            Some(o) => o,
            _ => {
                println!("couldnt access working dir: {}", &backup_job.src);
                return Err(());
            }
        };

        let mut handles = vec![];
        for sub_dir in sub_dirs{
            for dest in &backup_job.dests {
                for job in &backup_job.jobs {
                println!("generating jobs for {} - {} | {}", &backup_job.name, &job.name, &sub_dir);

                    //TODO: less then optimal
                    let i_src = path_join(&[ backup_job.src.clone(), sub_dir.clone() ]);
                    let i_dest = path_join(&[ dest.clone(), sub_dir.clone(), job.name.clone() ]);
                    let i_job = job.clone();
                    let i_ignore : Vec<String> = match &backup_job.ignore {
                        None => vec![],
                        Some(o) => o.clone() 
                    };
                    handles.push(tokio::spawn(async move {
                        create_job(i_src, i_dest, i_job, i_ignore).await
                    }));
                }
            }
        }

        for handle in handles{
            match handle.await {
                Ok(Some(o)) => jobs.push(o),
                _ => {
                    println!("Failed one Job at: {}", &backup_job.name);
                    continue;
                }
            }
        }

        
    }

    { //spawn jobs
        let mut handles = vec![];
        for job in jobs {
            println!("Fire up job {} -> {}", &job.src, &job.dest);
            handles.push(tokio::spawn(async move {
                run_job(job).await;
            }));
        }

        for handle in handles{
            match handle.await {
                Ok(_) => {},
                _ => {
                    println!("Failed one Job: {}", &backup_job.name);
                    continue;
                }
            }
        }


    }

    match &backup_job.args { //after working
        Some(args) =>  match &run_command_list(&args.after_access_working).await {
            Ok(_) => {},
            _  => return Err(())
        },
        _ => {}
    }

    match &backup_job.args { //after backup
        Some(args) =>  match &run_command_list(&args.after_access_backups).await {
            Ok(_) => {},
            _  => return Err(())
        },
        _ => {}
    }

    Ok(())
}

async fn run_job(job: BackupJob) -> Result<(),()>{
    match &job.delete { //delete old
        Some(path) => {
            println!("Cleanup: {}", path);
            match Command::new("rclone").arg("purge").arg(&path).spawn() {
                Ok(mut o) => match o.wait().await {
                    Ok(_) => {},
                    _ => {
                        println!("Failed cleanup: {}", &path);
                        return Err(());
                    }

                },
                _ => {
                    println!("Failed cleanup: {}", &path);
                    return Err(());
                }
            }
        },
        None => {}
    }

    let i_src = job.src.clone();
    let i_ignore = job.ignore.clone();
    let backup = tokio::spawn( async move {
        BackupInfo::from_working_dir(&i_src, &i_ignore).await
    });

    let root = match TempDir::new("butter_tmp"){
        Ok(o) => o,
        Err(_) => {
            println!("couldnt create tmp dir");
            return Err(());
        }
    };
    let backup_file_path = root.path().join("backup.json");

    
    let mut backup : BackupInfo = match backup.await {
        Ok(Ok(o)) => o,
        _ => {
            println!("couldnt create backup");
            return Err(());
        }
    };
    match &job.bu_type { // calc increment
        BackubJobType::Inc {base} => {
            let base_text = match Command::new("rclone").arg("cat").arg(&base).output().await {
                Ok(o) => {
                    if o.status.success() {
                        o.stdout
                    } else {
                        println!("couldnt read file: {}", &base);
                        return Err(());
                    }
                },
                _ => {
                    println!("couldnt read file: {}", &base);
                    return Err(());
                }
            };
            let base : BackupInfo = match serde_json::from_slice(&base_text) {
                Ok(o) => o,
                _ => {
                    println!("couldnt parse file: {}", &base);
                    return Err(());
                }
            };
            
            (backup, _) = backup.calc_increment(base).await;
        },
        _ => {}
    }


    { // writing backupinfo to file
        let mut backup_file = match File::create(&backup_file_path).await {
            Ok(o) => o,
            _ => {
                println!("couldnt create tmp file");
                return Err(());
            }
        };
        let info = match serde_json::to_vec_pretty(&backup) {
            Ok(o) => o,
            _ => {
                println!("failed serializing backupinfo");
                return Err(());
            }
        };
        println!("{}", String::from_utf8_lossy(&info));
        match backup_file.write_all(&info).await {
            Ok(_) => {},
            _ => {
                println!("failed writing to tmp file");
                return Err(());
            }
        }
    }


    let mut file_task = match Command::new("rclone").arg("copyto").arg(&backup_file_path).arg(job.dest.clone() + ".json").spawn(){
        Ok(o) => o,
        _ => {
            println!("failed spawning copy task for backup info");
            return Err(())
        }
    };

    match &job.bu_type {
        BackubJobType::Full => {
            let mut src = Command::new("rclone");
            src.arg("copy");
            src.arg(&job.src);
            src.arg(&job.dest);

            for ig in job.ignore {
                src.arg(format!("--exclude={}",ig));
            }
            
            let mut src = match src.spawn(){
                Ok(o) => o,
                _ => {
                    println!("failed spawning copy task for working dir");
                    return Err(())
                }
            };

            match src.wait().await {
                Ok(_) => {},
                _ => {
                    println!("failed copying src to {}", &job.dest);
                    return Err(())
                }
            }
        },
        BackubJobType::Inc {base} => {
            let mut copy_handles = vec![];
            let added = match &backup.increment_delta{
                Some(o) => &o.added,
                _ => {
                    println!("backup job increment without calculated increment");
                    return Err(());
                }
            };
            copy_to_list(&job.src, &job.dest, &added, &mut copy_handles).await;
            let edited = match &backup.increment_delta{
                Some(o) => &o.edited,
                _ => {
                    println!("backup job increment without calculated increment");
                    return Err(());
                }
            };
            copy_to_list(&job.src, &job.dest, &edited, &mut copy_handles).await;

            for mut copy_handle in copy_handles{
                match copy_handle.wait().await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Err while increment copies: {}", e);
                        continue;
                    }
                }
            }

        }
    }

    match file_task.wait().await {
        Ok(_) => {},
        _ => {
            println!("failed copying backupinfo to {}", (job.dest.clone() + ".json"));
            return Err(())
        }
    }

    Err(())
}

async fn copy_to_list(src_root: &String, dest_root: &String, copy: &Vec<String>, handles: &mut Vec<tokio::process::Child>){
    for x in copy{
        match Command::new("rclone").arg("copyto")
            .arg(path_join(&[src_root.clone(), x.clone()]))
            .arg(path_join(&[dest_root.clone(), x.clone()]))
            .spawn() {
                Ok(o) => handles.push(o),
                _ => {
                    println!("!Failed spawning copy for {}", path_join(&[src_root.clone(), x.clone()]));
                }
        } 
    }
}

async fn create_job(src: String, dest: String, backup_job: Job, ignore: Vec<String>) -> Option<BackupJob> {
    let now = Utc::now().naive_utc();
    let now_str = now.format("%Y%m%d:%H%M%S").to_string();
    let now_date = now.date();

    let dir_names = match create_dir_and_get_dir_names(&dest).await {
        Some(o) => o,
        _ => {
            return None
        }
    };
    if dir_names.len() == 0 {
        return Some(BackupJob{
            src: src.clone(),
            dest: path_join(&[dest.clone(),now_str.clone(), now_str]),
            bu_type: BackubJobType::Full,
            delete: None,
            ignore
        });
    }
    let last_collection_name = &dir_names[dir_names.len()-1];
    let last_collection_path = path_join(&[dest.clone(), last_collection_name.clone()]);
    let last_collection = match create_dir_and_get_dir_names(&last_collection_path).await {
        Some(o) => o,
        _ => {
            return None
        }
    };
    let last_backup_name = &last_collection[last_collection.len() - 1];


    let last_backup_time = 
        match NaiveDateTime::parse_from_str(&last_backup_name, "%Y%m%d:%H%M%S") {
            Ok(o) => o.date(),
            _ => {
                println!("corrupted backup dir (could not parse time) => {} / {} / {}", &dest, &last_collection_name, &last_backup_name);
                return None;
            }
    };
    
    match backup_job.typ.as_str() {
        "all" => {},
        "day" => {
            if last_backup_time + RelativeDuration::days(1) > now_date {
                return None;
            }
        },
        "week" => {
            if last_backup_time + RelativeDuration::weeks(1) > now_date {
                return None;
            }
        },
        "month" => {
            if last_backup_time + RelativeDuration::months(1) > now_date {
                return None;
            }
        },
        "year" => {
            if last_backup_time + RelativeDuration::years(1) > now_date {
                return None;
            }
        },
        _ => {
            println!("unknown backup type: {}", &backup_job.typ);
            return None;
        }
    }

    if last_collection.len() == 0 {
        println!("corrupted backup dir (found collection without base) => {} / {}", &dest, &last_collection_name);
        return None;
    }

    if last_collection.len() - 1 < backup_job.inc_count as usize {
        //increment
        return Some(BackupJob{
            src: src.clone(),
            dest: path_join(&[dest.clone(), last_collection_name.clone(), now_str]),
            bu_type: BackubJobType::Inc{ 
                base: path_join(&[dest.clone(), last_collection_name.clone(), last_backup_name.clone()]) + ".json"
            },
            delete: None,
            ignore

        });
    } else {
        //full
        if dir_names.len() <= backup_job.base_count as usize {
            return Some(BackupJob{
                src: src.clone(),
                dest: path_join(&[dest.clone(),now_str.clone(), now_str]),
                bu_type: BackubJobType::Full,
                delete: None,
                ignore
            });
        } else {
            return Some(BackupJob{
                src: src.clone(),
                dest: path_join(&[dest.clone(), now_str.clone(), now_str]),
                bu_type: BackubJobType::Full,
                delete: Some(path_join(&[dest.clone(), dir_names[0].clone()])),
                ignore
            });
        }
    }
}

async fn create_dir_and_get_dir_names(root: &String) -> Option<Vec<String>> {

    match Command::new("rclone").arg("mkdir").arg(&root).spawn() {
        Ok(mut o) => match o.wait().await {
           Ok(_) => {},
           _ => {
               println!("could not make dir: {}", &root);
               return None;
           }
        },
        _ => {
            println!("could not make dir: {}", &root);
            return None;
        }
    }
    let dirs = match Command::new("rclone").arg("lsjson").arg(&root).output().await {
        Ok(o) => {
            if o.status.success() {
                o.stdout
            } else {
                println!("failed reading dirs from: {}", &root);
                return None;
            }
        },
        _ => {
            println!("failed reading dirs from: {}", &root);
            return None;
        }
    };
    let dirs : RcloneLsjsonResult = match serde_json::from_slice(&dirs) {
        Ok(o) => o,
        _ => {
            println!("Failed parsing lsjson: {}", &root);
            return None;
        }
    };
    let mut dir_names = vec![];
    for dir in dirs{
        if dir.is_dir {
            dir_names.push(dir.name);
        }
    }
    dir_names.sort();
    Some(dir_names)
}


async fn run_command_list(args: &Arg) -> Result<(),()>{
    match &args {
        Some(cmds) => {
            for cmd in cmds {
                if cmd.len() == 0 {
                    continue;
                }
                let mut c = Command::new(&cmd[0]);
                for c_arg in &cmd[1..] {
                    c.arg(&c_arg);
                }
                let mut c = match c.spawn() {
                    Ok(o) => o,
                    Err(_) => {
                        println!("failed running: {:?}", &cmd);
                        return Err(());
                    }
                };
                match c.wait().await {
                    Ok(_) => {},
                    _ => {
                        println!("failed running: {:?}", &cmd);
                        return Err(());
                    }
                }
            }
        }, _ => {}
    }
    Ok(())
}

fn path_join(paths: &[String])->String{
    if env::consts::OS == "windows" {
        paths.join("\\")
    } else {
        paths.join("/")

    }
}










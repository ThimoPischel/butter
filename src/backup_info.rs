use std::collections::HashMap;

use anyhow::{bail, Context, Result};

use tokio::process::Command;
use tokio::task;

use super::helper::*;


#[derive(Debug, Deserialize, Serialize)]
pub struct PathInfo {
    pub mod_time: String,
    pub is_dir: bool,
    pub sha1sum: Option<String>,
}
impl PathInfo {}

#[derive(Debug, Deserialize, Serialize)]
pub struct IncrementDelta {
    pub added: Vec<String>,
    pub deleted: Vec<String>,
    pub edited: Vec<String>,
}
impl IncrementDelta {}

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupInfo {
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

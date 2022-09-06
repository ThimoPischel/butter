use anyhow::{bail, Context, Result};
use tokio::{io,fs};





pub type Arg = Option<Vec<Vec<String>>>;
#[derive(Serialize, Deserialize)]
pub struct Args {
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
pub struct ConfigItem {
    pub name: String,
    pub src: String,
    pub sub_dirs: Option<bool>,
    pub dests: Vec<String>,
    pub args: Option<Args>,
    pub jobs: Vec<Job>,
    pub ignore: Option<Vec<String>>,
}
pub type Config = Vec<ConfigItem>;

pub async fn read_config() -> Result<Option<Config>> {
    let config = match fs::read("/home/thimo/Config/butter/config.json").await {
        Ok(config) => config,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => bail!(err),
    };

    let config = serde_json::from_slice::<Config>(&config).context("Failed parsing config")?;

    Ok(Some(config))
}
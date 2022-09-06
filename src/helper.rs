use std::env;

pub fn path_join(paths: &[&str]) -> String {
    if env::consts::OS == "windows" {
        paths.join("\\")
    } else {
        paths.join("/")
    }
}


#[derive(Deserialize)]
pub struct RcloneLsjsonItem {
    #[serde(rename = "Path")]
    pub path: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Size")]
    pub size: i64,
    #[serde(rename = "MimeType")]
    pub mime_type: String,
    #[serde(rename = "ModTime")]
    pub mod_time: String,
    #[serde(rename = "IsDir")]
    pub is_dir: bool,
}
pub type RcloneLsjsonResult = Vec<RcloneLsjsonItem>;

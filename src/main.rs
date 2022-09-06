#[macro_use]
extern crate serde_derive;

mod execute_build_backups;
mod execute_check_structure;
mod execute_create_structure;
mod execute_recover;
mod helper;
mod config;
mod backup_info;

use std::env;
use anyhow::{Result};
use execute_build_backups::*;

#[tokio::main]
async fn main() -> Result<()> {

    let args: Vec<String> = env::args()
        .skip(1)
        .collect();

    if args.len() == 0 {
        return execute_build_backups(args).await;
    }

    match args[0].as_str(){
        "run" => return execute_build_backups(args).await,
        _ => {}
    }

    Ok(())
}



# Butter (WIP)

## WIP
Roadmap:
1. craeting full backups
2. creating incremental (Filebased) backups
3. restore manager
4. import old backups (into config)
5. optional system shutdown after finishing

## Config (~/.config/butter/config.json)

### Base configuration:
``` json
[

    {   
        "name": "name of backup",
        "src": "/path/to/your/source/to/backup",

        "dests": [
            "/path/to/your/destination/for/the/backup/1",
            "/path/to/your/source/to/backup"
        ],
        "jobs":[
            {
                "name": "your name of the job",
                "typ": "all|day|week|month|year",
                "base_count": 2,
                "inc_count": 3
            }
        ]
    }
]

```



### Optional:

#### Args
They will be execute bevore/after accessing the working/backup data.
Each arg in "args" is optional is not needed to set.
``` json
"args": {
    "bevore_access_backups":  [
      ["ls", "-a"],
    ],
    "after_access_backups": [ ],
    "bevore_access_working": [ ],
    "after_access_working": [ ],
}
```

#### Ignore
Default rclone excludes (--exclude=_____)
```json
"ignore": [
  ".git/**",
  "target/**"
]
```

#### Subdirs
This option is only for dirs that only contains dirs. It will perform the backup for all subdirs.
```json
"sub_dirs": true
```







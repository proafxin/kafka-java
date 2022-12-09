# Kafka Application in Java
This is a demo project on using Kafka in Java.

# Environment Setup

## Windows

Download kafka from official Apache Kafka website. Extract it and  rename it to `kafka`. Put `kafka` in `C:` drive.
Inside `kafka` directory, create a directory `data`. Go into `data/` and create two more directories: `server/` and `zookeeper`.
Open `config/zookeeper.properties` and rewrite `dataDir=C:\kafka\data\zookeeper`
Open `config/server.properties` and rewrite `log.dirs=C:\kafka\data\server`

From `C:\kafka\bin\windows\` directory, run `.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties`
Check that zookeeper is running on port 2181.
From same directory, run `.\bin\windows\kafka-server-start.bat .\config\server.properties`


## Linux

Follow the exact same steps as windows except run the corresponding `.sh` scripts from `kafka/bin/` directory.

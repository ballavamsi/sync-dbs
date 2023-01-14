This project syncs 2 mysql databases.

It is scheduled to run every 24hours.

## How to use

Create below folder structure

```
/data
    /backup
    dbs.yaml
```

Create a file named dbs.yaml with below content
Make sure that both databases are of same version

```
- name: db1
      source:
        host: db_host2
        user: user2
        password: pwd2
        database: db_name2
        port: db_port2
      destination:
        host: localhost
        user: user1
        password: pwd1
        database: db_name1
        port: db_port1
```

Run the docker container

```
docker run -d -v /data:/data -e DBS_FILE=/data/dbs.yaml -e BACKUP_DIR=/data/backup --name db-sync --restart=always db-sync
```

or use docker-compose.yml file in the project

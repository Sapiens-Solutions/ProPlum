# Sapiens Solutions Greenplum ETL framework

## Content
The repository contains sources of the Greenplum database framework for data extraction, loading and transformation

## Installation 
1. Download and install Liquibase from liquibase.org
2. Clone the repository to a local folder
3. Switch to the folder and run the command:
```bash
liquibase update --changelog-file=install.yaml --url="jdbc:postgresql://host:port/database" --username="user" --password="password" -Dtarget_schema="target_schema" -Downer="owner"
```
where:
`host` - greenplum host
`port` - greenplum port (usually 5432)
`database` - database in your greenplum
`user` - user to connect greenlum
`password` - user's password to connect greenlum
`target_schema` - schema where framework will be installed
`owner` - user of framework (for grant command)

4. Set up the foreign server for logging (fw/servers/server.sql)
5. Follow the instructions in Airflow folder to configure airflow DAGs

## License
Apache 2.0

## Contacts
info@sapiens.solutions
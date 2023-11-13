# Sapiens Solutions Greenplum ETL framework

## Content
The repository contains sources of the Greenplum database framework for data extraction, loading and transformation

## Installation 
1. Download and install Liquibase from liquibase.org
2. Clone the repository to a local folder
3. Switch to the folder and run the command:
liquibase update --changelog-file=install.yaml --url="jdbc:postgresql://<host>:<port>/<database>" --username="<user>" --password="password" -Dtarget_schema="<target_schema>" -Downer="<owner>"
4. Set up the foreign server for logging (fw/servers/server.sql)

## License
Apache 2.0

## Contacts
info@sapiens.solutions
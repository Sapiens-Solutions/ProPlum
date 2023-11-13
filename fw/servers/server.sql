CREATE SERVER adwh_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '<your greenplum server>', port '5432', dbname '<your greenplum database>');

CREATE USER MAPPING FOR "<your log user>"
        SERVER adwh_server
        OPTIONS (user 'your log user', password 'password');

# LH_ED_ISACJFS

This project is a Python-based tool for backing up and restoring PostgreSQL databases. It provides functionality for creating SQL backups, copying CSV files, restoring SQL backups, creating tables based on CSV schemas, and loading CSV data into database tables.

## Installation

To use this project, you need to have Python installed. Clone the repository to your local machine and install the required dependencies by running the following command:
```commandline
pip install -r requirements.txt
```
And also needs a postgres database:

## Creating a PostgreSQL Database

Before running the application, you need to create a PostgreSQL database. Follow the instructions below to create the database:

1. Make sure you have PostgreSQL installed on your system. If you don't have it, you can download and install it from [https://www.postgresql.org](https://www.postgresql.org).

2. Open a terminal or command prompt and connect to the PostgreSQL server using your user credentials.

3. Create a new database by executing the following SQL command:

   ```sql
   CREATE DATABASE database_name;
   ```

   Replace `database_name` with the desired name for your database.

4. Optionally, you can create a new schema within the database by executing the following SQL command:

   ```sql
   CREATE SCHEMA public;
   ```

5. Make sure to configure the appropriate permissions for the user that will be used to access the database.

6. Update the connection settings in the Python application to reflect the correct database name, schema, and credentials.

7. You are now ready to run the application and interact with the PostgreSQL database.

If you already have an existing PostgreSQL database that you want to use, skip steps 3 and 4 and simply update the connection settings in the application.

## Add Tool Locations to the PATH Environment Variable

This is necessary for correct execution of pg_dump and pg_restore commands.

1. On the Start menu, right-click Computer.

2. On the context menu, click Properties.

3. In the System dialog box, click Advanced system settings.

4. On the Advanced tab of the System Properties dialog box, click Environment Variables.

5. In the System Variables box of the Environment Variables dialog box, scroll to Path and select it.

6. Click the lower of the two Edit buttons in the dialog box.

7. In the Edit System Variable dialog box, scroll to the end of the string in the Variable value box and add a semicolon (;).

8. Add the new path after the semicolon.
```
C:\Program Files\PostgreSQL\15\bin
```   

9. Click OK in three successive dialog boxes, and then close the System dialog box.

font: [https://learn.microsoft.com](https://learn.microsoft.com/en-us/previous-versions/office/developer/sharepoint-2010/ee537574(v=office.14)#to-add-a-path-to-the-path-environment-variable)

## Usage

The project consists of the following classes:

- `SaveDB`: This class is responsible for creating backups of a PostgreSQL database. It provides methods for saving SQL backups and copying CSV files.

- `LoadDB`: This class is responsible for restoring a PostgreSQL database from backups and loading CSV data into database tables. It provides methods for restoring SQL backups, creating tables based on CSV schemas, and loading CSV data.

- `NewUser`: This class extends the `LoadDB` class and provides additional functionality for altering the database user. This class should be used when there is a need to restore the database to a new one where the nortw user does not exist and the following error occurs:
```commandline
pg_restore: error: could not execute query: ERROR:  role "northwind_user" does not exist
Command was: ALTER DATABASE northwind OWNER TO northwind_user
```

### SaveDB Class

To create a backup of a PostgreSQL database, instantiate the `SaveDB` class with the following parameters:

```python
SaveDB(user, host, port, dbname, password, backup_dir, csv_dir)
```

- `user` (str): The username for connecting to the database.
- `host` (str): The host address of the database server.
- `port` (str): The port number for the database connection.
- `dbname` (str): The name of the database.
- `password` (str): The password for the database user.
- `backup_dir` (str): The directory where the backups will be saved.
- `csv_dir` (str): The directory containing the CSV files to be backed up.

Example usage:

```python
save_db = SaveDB('user', 'localhost', '5432', 'mydatabase', 'password', '/path/to/backup', '/path/to/csv')
save_db.save_sql()  # Save a SQL backup
save_db.save_csv()  # Save a CSV backup
```
Note: SaveDB creates a path from the given backup_dir:

```python
'[backup_dir]+/data/postgres/{table}/2021-01-01/file.format'
'[backup_dir]+/data/csv/2021-01-02/file.format'
```

### LoadDB Class

To restore a PostgreSQL database from backups and load CSV data, instantiate the `LoadDB` class with the following parameters:

```python
LoadDB(user, host, port, dbname, password, backup_dir, date)
```

- `user` (str): The username for connecting to the database.
- `host` (str): The host address of the database server.
- `port` (str): The port number for the database connection.
- `dbname` (str): The name of the database.
- `password` (str): The password for the database user.
- `backup_dir` (str): The directory where the backups are stored.
- `date` (str): The date of the backups to restore.

Example usage:

```python
load_db = LoadDB('user', 'localhost', '5432', 'mydatabase', 'password', '/path/to/backup', '2022-01-01')
load_db.restore_sql()    # Restore a SQL backup
load_db.schema_csv()     # Create a table based on CSV schema
load_db.load_csv_data()  # Load CSV data into the table
```


### NewUser Class

To create a new database user, instantiate the `NewUser` class with the same parameters as the `LoadDB` class:

```python
NewUser(user, host, port, dbname, password, backup_dir, date)
```

Example usage:

```python
new_user = NewUser('user', 'localhost', '5432', 'mydatabase', 'password', '/path/to/backup', '2022-01-01')
new_user._alter_user('new_user')  # Create a new database user
```

## License

This project is licensed under the [MIT License](LICENSE).
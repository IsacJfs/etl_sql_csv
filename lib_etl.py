import subprocess
import os
import time
import logging
import shutil
import socket
import psycopg2

# Configurar o logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Criar um handler para exibir o log no console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Definir o formato da mensagem de log
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

# Adicionar o handler ao logger
logger.addHandler(console_handler)


class SaveDB:
    def __init__(self, user: str, host: str, port: str, dbname: str, password: str, backup_dir: str, csv_dir: str):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        self._csv_dir = csv_dir
        self._save_dir = backup_dir
        self._conn_test()
        self._putenv()
        self._pg_dump_path, self._csv_save_path = self._dir_constructor()
        self._str_constructor()

    def _conn_test(self) -> None:
        """
        Test the connection to the SQL server.

        This method tests the connection to the SQL server using the socket library.
        It creates a socket and attempts to connect to the specified host and port.
        The result of the connection test is logged.

        Returns:
            None
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self._host, int(self._port)))
            logger.info(f"Connection success. {self._host}:{self._port}")
        except ConnectionRefusedError:
            logger.warning(f"Unable to connect for: {self._host}:{self._port}", exc_info=True)
        finally:
            # Close socket
            sock.close()

    def _str_constructor(self) -> str:
        """
        Construct the pg_dump command string.

        This method constructs the pg_dump command string based on the specified attributes and options.
        The constructed command string is returned.
            pg_dump: postgres function
            -U: DATABASE_USER
            -Z: COMPRESSION -> int(0-9)
            -f: PATH LOCATION TO SAVE
            -F: Format to make a pg_dump
            DATABASE_NAME
        Returns:
        str: The pg_dump command string.
        """
        dumper = (f"pg_dump "
                  f"-U {self._user} "
                  f"-Z 9 "
                  f"-f {self._pg_dump_path}pgdump_{self._dbname}.sql.pgdump "
                  f"-F c "
                  f"{self._dbname}")
        return dumper

    def _today(self) -> str:
        """
        Get the current date.

        This method returns the current date in the format "Year-month-day" as a string.

        Returns:
            str: The current date in the format "Year-month-day".
        """
        todays = time.strftime('%Y-%m-%d')
        return todays

    def _dir_constructor(self) -> tuple:
        """
        Creates the folders for saving backups.

        This method creates the necessary directories for saving backups based on the specified 'save_dir'.
        The 'os.makedirs()' function from the 'os' module is used to create the directories.

        Args:
            self

        Returns:
            tuple: A tuple containing the paths for saving the backups.
                The first element is the path for saving the SQL backup file in the format:
                "[save_dir]/data/postgres/{table}/{date=self.today}/file.format".
                The second element is the path for saving the CSV backup file in the format:
                "[save_dir]/data/csv/{date=self.today}/file.format".

        Raises:
            OSError: If there is an issue with the specified 'save_dir' path.

        Note:
            This method is used internally and should not be called directly by the user.
        """
        try:
            logger.info("Making directories to save files")
            postgres_path = f'{self._save_dir}data\\postgres\\{self._dbname}\\{self._today()}\\'
            csv_name = self._csv_dir[self._csv_dir.rfind('\\') + 1:-4]
            csv_path = f'{self._save_dir}data\\csv\\{self._today()}\\'
            os.makedirs(postgres_path, exist_ok=True)
            logger.info(f"Select path {postgres_path} to save a SQL backup")
            os.makedirs(csv_path, exist_ok=True)
            logger.info(f"Select path {csv_path} to save a CSV backup")
            return postgres_path, csv_path
        except OSError as e:
            logger.warning(f"There is a problem with path {self._save_dir}. \n OSError: {e}")

    def _putenv(self):
        """
        Set the PGPASSWORD environment variable.

        This method sets the PGPASSWORD environment variable to the specified password
        value, which will be used for authentication when connecting to a PostgreSQL database.

        Note:
            This method is used internally and should not be called directly by the user.

        Raises:
            None

        Returns:
            None
        """
        os.putenv('PGPASSWORD', self._password)

    def save_sql(self) -> None:
        """
        Save a SQL backup of the database.

        This method initiates the `pg_dump` process to create a SQL backup of the specified database.
        The command to execute `pg_dump` is constructed using the `_str_constructor()` method.
        The backup process is logged using the logger.

        Returns:
            None

        Raises:
            ValueError: If an error occurs while executing the `pg_dump` command.

        Note:
            The timeout for the `pg_dump` command is set to 10 seconds.
        """
        logger.info(f"Database {self._dbname} pg_dump process started ")
        command = self._str_constructor()

        try:
            logger.info(f"Execute {command}")
            do_command = subprocess.run(command, shell=True, timeout=10)
            if do_command.returncode == 0:
                return logger.info("pg_dump was a success")
            else:
                raise ValueError

        except ValueError as e:
            print(f"An error occurred while executing the command: {command}. \n"
                  f"ValueError: {e}")

        except subprocess.TimeoutExpired as te:
            print(f"Timeout expired, incorrect parameters. \n"
                  f"subprocess.TimeoutExpired: {te}")

    def save_csv(self) -> None:
        """
        Save a CSV backup file.

        This method performs a backup of the CSV file by copying it to the specified CSV save path.
        The origin file and the destination path are logged using the logger.

        Returns:
            None

        Raises:
            ValueError: If an error occurs during the file copy process.

        Note:
            The `_csv_dir` attribute should contain the path to the original CSV file.
        """
        logger.info("csv file backup process started")
        try:
            origin_file = self._csv_dir
            shutil.copy(origin_file, self._csv_save_path)
            return logger.info("csv backup was a succes")
        except ValueError as e:
            print(f"An error prevents copying the file. \n"
                  f"ValueError: {e}")


class LoadDB:
    def __init__(self, user: str, host: str, port: str, dbname: str, password: str, backup_dir: str, date: str):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._password = password
        self._backup_dir = backup_dir
        self._date = date
        self._putenv()
        self.sql_path, self.csv_path = self._path_constructor()

    def _connection(self):
        """
        Establish a PostgreSQL database connection.

        This method establishes a connection to the PostgreSQL database using the specified host, port, database name,
        username, and password. The connection object is returned as a string.

        - *dbname*: the database name
        - *database*: the database name (only as keyword argument)
        - *user*: user name used to authenticate
        - *password*: password used to authenticate
        - *host*: database host address
        - *port*: connection port number (defaults to 5432 if not provided)

        Returns:
            psycopg2.extensions.connection: The connection object representing the established database connection.

        Note:
            The connection object returned is an instance of the `psycopg2.extensions.connection` class.
        """
        conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._dbname,
            user=self._user,
            password=self._password
        )
        return conn

    def _putenv(self):
        os.putenv('PGPASSWORD', self._password)

    def _str_constructor(self) -> str:
        """
        Construct the pg_restore command string.

        This method constructs the pg_restore command string based on the specified attributes,
        including the database name, host, port, username, and the path to the SQL file.
        The constructed command string is logged and returned.

        Returns:
            str: The pg_restore command string.

        Note:
            The SQL file path should be provided in the `self.sql_path` attribute.
        """
        restore = (f"pg_restore "
                   f"-d {self._dbname} "
                   f"-h {self._host} "
                   f"-p {self._port} "
                   f"-U {self._user} "
                   f"{self.sql_path} ")
        logger.info(f"Create restore command: {restore}")
        return restore

    def _path_constructor(self):
        """
        Construct the file paths for SQL and CSV backups.

        This method constructs the file paths for SQL and CSV backups based on the specified backup directory
        and date. It searches for files in the directory and appends the matching files to a list.
        The SQL and CSV file paths are determined from the list of paths.

        Returns:
            tuple: A tuple containing the file paths for SQL and CSV backups.
                The first element is the SQL file path.
                The second element is the CSV file path.

        Raises:
            ValueError: If no SQL file is found in the backup directory.

        Note:
            The backup directory and date should be set in the corresponding attributes.
        """
        logger.info(f"Initiated file location")
        path = self._backup_dir
        date = self._date
        paths_list = []
        sql_path = ""
        csv_path = ""
        try:
            for dir_path, dir_names, file_names in os.walk(path):
                for filename in file_names:
                    file = os.path.join(dir_path, filename)
                    if date in file:
                        paths_list.append(file)
            logger.info(f"Caminhos: {paths_list}")
            for valor in paths_list:
                if valor.endswith(".csv"):
                    csv_path = valor
                elif valor.endswith(".sql.pgdump"):
                    sql_path = valor
        except ValueError as e:
            logger.warning(f"File locarion error."
                        f"ValueError: {e}")
        logger.info(f"SQL pg_dump located in: {sql_path}")
        logger.info(f"CSV backup located in: '{csv_path}'")
        return sql_path, csv_path

    def restore_sql(self):
        """
        Restore a SQL backup.

        This method initiates the SQL restore process by executing the specified command string.
        The command string is constructed using the `_str_constructor()` method.
        The restore process is logged using the logger.

        Returns:
            None

        Raises:
            None

        Note:
            The timeout for the restore process is set to 30 seconds.
        """
        logger.info(f"Initiated SQL restore")
        command = self._str_constructor()
        logger.info(f"Do a command: {command}")
        try:
            subprocess.run(command, shell=True, timeout=30)
        except subprocess.TimeoutExpired:
            logger.info(f"Command fail")

    def schema_csv(self):
        """
        Create a new table 'order_details' based on the CSV schema.

        This method creates a new table 'order_details' in the 'public' schema with the specified columns and constraints.
        The SQL command string for creating the table is constructed within the method.
        The command is executed using the database connection, and the table creation process is logged.

        Returns:
            None

        Raises:
            None

        Note:
            The table creation command should match the desired schema based on the CSV data.

            Example schema:
            CREATE TABLE public.order_details (
                order_id smallint NOT NULL,
                product_id smallint NOT NULL,
                unit_price real,
                quantity smallint NOT NULL,
                discount real
            );

            ALTER TABLE public.order_details
            ADD CONSTRAINT pk_order_details PRIMARY KEY (order_id, product_id);

            ALTER TABLE public.order_details
            ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (order_id) REFERENCES orders(order_id);

            ALTER TABLE public.order_details
            ADD CONSTRAINT fk_order_details_products FOREIGN KEY (product_id) REFERENCES products(product_id);
        """
        logger.info(f"Create new Table: 'order_details'")
        command = ("""
        CREATE TABLE public.order_details (
        order_id smallint NOT NULL,
        product_id smallint NOT NULL,
        unit_price real,
        quantity smallint NOT NULL,
        discount real
        );
        ALTER TABLE public.order_details 
        ADD CONSTRAINT pk_order_details PRIMARY KEY (order_id, product_id);
        ALTER TABLE public.order_details 
        ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (order_id) REFERENCES orders(order_id);
        ALTER TABLE public.order_details 
        ADD CONSTRAINT fk_order_details_products FOREIGN KEY (product_id) REFERENCES products(product_id);
        """)

        conn = self._connection()
        cur = conn.cursor()
        logger.info(f"Execute SQL COMMAND: {command}")
        cur.execute(command)
        time.sleep(5)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"The creation of the Table 'order_datails' was a success")

    def load_csv_data(self):
        """
        Load CSV data into a database table.

        This method loads the data from a CSV file into a specified database table.
        The CSV file path and table name are retrieved from the corresponding attributes.
        The `COPY` command is constructed using the CSV file path and table name.
        The command is executed using the database connection, and the data loading process is logged.

        Returns:
            None

        Raises:
            None

        Note:
            - The CSV file path should be set in the `csv_path` attribute.
            - The table name should be provided as a string in the `table_name` attribute.
            - The CSV file should have a header row.

        Example command:
        COPY order_details FROM '/path/to/csv/file.csv' DELIMITER ',' CSV HEADER;
        """
        csv_path = self.csv_path
        table_name = 'order_details'
        command = f"COPY {table_name} FROM '{csv_path}' DELIMITER ',' CSV HEADER;"
        conn = self._connection()
        cur = conn.cursor()
        logger.info(f"Execute SQL COMMAND: {command}")
        cur.execute(command)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"CSV copied to DataBase {self._dbname}")


class NewUser(LoadDB):
    def __init__(self, user: str, host: str, port: str, dbname: str, password: str, backup_dir: str, date: str):
        super().__init__(user, host, port, dbname, password, backup_dir, date)

    def _alter_user(self, new_user):
        """
        Alter the database user.

        This method alters the database user to create a new user with the specified username and password.
        If the specified user differs from the current user, a new user is created and logged.
        The new user is assigned the provided password.

        Returns:
            None
        """
        backup_user = new_user
        if backup_user != self._user:
            logger.info(f"Create new user: {backup_user}")
            command_sql = f"CREATE ROLE {backup_user} LOGIN PASSWORD '{self._password}'"
            conn = self._connection()
            cur = conn.cursor()
            cur.execute(command_sql)
            time.sleep(2)
            conn.commit()
            cur.close()
            conn.close()
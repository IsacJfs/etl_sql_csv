from lib_etl import SaveDB, LoadDB, NewUser
import time

command_save = SaveDB(
    user="northwind_user",
    host="localhost",
    port="5432",
    dbname="northwind",
    password="thewindisblowing",
    backup_dir="E:\\etl_sql_csv\\",
    csv_dir="E:\\indicium_desafios\\code-challenge\\data\\order_details.csv"
)
time.sleep(5)

command_save.save_sql()
command_save.save_csv()

time.sleep(5)

command_restore = LoadDB(
    user="user",
    host="localhost",
    port="5432",
    dbname="dbname",
    password="password",
    backup_dir="E:\\etl_sql_csv\\",
    date="2023-07-14"
)

command_restore.restore_sql()
time.sleep(5)
command_restore.schema_csv()
time.sleep(5)
command_restore.load_csv_data()

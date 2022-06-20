import pandas as pd
import sqlite3
import datetime


def column_load_log(column):
    date = datetime.datetime.now()
    column_log = f"{date.strftime('%x')} - {date.strftime('%X')}: " \
                 f"Added {column} column.++++++"
    print(column_log)
    with open("load.log", "a") as f:
        f.write(column_log + '\n')


def record_load_log(n):
    date = datetime.datetime.now()
    record_log = f"{date.strftime('%x')} - {date.strftime('%X')}: " \
                 f"Added {n} offers.++++++"
    print(record_log)
    with open("load.log", "a") as f:
        f.write(record_log + '\n')


def run():
    con = sqlite3.connect('dwh.db')
    con.execute("CREATE TABLE IF NOT EXISTS 'offers' (id integer primary key autoincrement);")
    staging_df = pd.read_csv('staging.csv', index_col=0)

    schema_list = list(staging_df.columns)

    for column in schema_list:
        try:
            con.execute(f"ALTER TABLE 'offers' ADD COLUMN '{column}' integer;")
            column_load_log(column)
        except sqlite3.OperationalError:
            print(f"{column} is already in table ########################")

    staging_df.to_sql('offers', con=con, if_exists='append', index=False)
    record_load_log(len(staging_df))

    con.commit()
    con.close()


if __name__ == '__main__':
    run()


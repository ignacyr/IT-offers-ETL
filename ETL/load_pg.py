import pandas as pd
import datetime
import psycopg2


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
    conn = psycopg2.connect(
        dbname="dwh",
        user="app_user",
        host="it-offers.c9umk0ew1r8h.us-east-1.rds.amazonaws.com",
        password="analyze2137"
    )
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS offers (id serial primary key);")
    staging_df = pd.read_csv('staging.csv', index_col=0)

    schema_list = list(staging_df.columns)

    for column in schema_list:
        try:
            cur.execute(f"ALTER TABLE offers ADD COLUMN {column} integer;")
            column_load_log(column)
        except psycopg2.OperationalError as error:
            print(f"{error}: {column} is already in table ########################")

    # staging_df.to_sql('offers', con=conn, if_exists='append', index=False)
    record_load_log(len(staging_df))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    run()


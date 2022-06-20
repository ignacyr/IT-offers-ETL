import pandas as pd
import datetime
import psycopg2
import sqlalchemy


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
    with open("/home/ignacyr/Desktop/password_app_user") as f:
        password_app_user = f.readline().strip()

    conn = sqlalchemy.create_engine(
        f'postgresql://app_user:{password_app_user}@it-offers.c9umk0ew1r8h.us-east-1.rds.amazonaws.com:5432/dwh')

    # conn = psycopg2.connect(
    #     dbname="dwh",
    #     user="app_user",
    #     host="it-offers.c9umk0ew1r8h.us-east-1.rds.amazonaws.com",
    #     password=password_app_user
    # )

    # cur = conn.cursor()
    conn.execute("CREATE TABLE IF NOT EXISTS offers (id serial primary key);")
    staging_df = pd.read_csv('staging.csv', index_col=0)

    schema_list = list(staging_df.columns)

    for column in schema_list:
        try:
            if column in ("min_salary", "max_salary", "date"):
                conn.execute(f"ALTER TABLE offers ADD COLUMN {column} integer;")
            elif column in ("title", "level", "skills", "category", "company", "link",
                            "trainee", "junior", "mid", "senior", "expert"):
                conn.execute(f"ALTER TABLE offers ADD COLUMN {column} varchar;")
            else:
                conn.execute(f"ALTER TABLE offers ADD COLUMN {column} boolean;")
            column_load_log(column)
        except psycopg2.OperationalError as error:
            print(f"{column} is already in table ########################")
        except sqlalchemy.exc.ProgrammingError as error:
            print(f"{column} is already in table ########################")

    # staging_df.to_sql('offers', con=conn, if_exists='append', index=False)
    staging_df.to_sql(name='offers', con=conn, if_exists='append', index=False)
    record_load_log(len(staging_df))


if __name__ == '__main__':
    run()


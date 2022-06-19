import pandas as pd
import sqlite3


def run():
    con = sqlite3.connect('dwh.db')
    staging2_df = pd.read_csv('staging2.csv', index_col=0)
    staging2_df.to_sql('dwh.db', con=con, if_exists='append')
    con.commit()
    con.close()


if __name__ == '__main__':
    run()


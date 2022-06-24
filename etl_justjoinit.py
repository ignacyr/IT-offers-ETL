import logging
import pandas as pd
import sqlalchemy


class ETLjustjoinit:
    def __init__(self, first_url: str, db_url: str, test_pages=1, is_that_test=True):
        self.test = is_that_test
        self.test_pages = test_pages
        self.url = first_url
        self.db_url = db_url
        logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s:%(levelname)s: %(message)s')

    def __extract(self):
        pass

    def __transform(self):
        pass

    def __load(self):
        logging.info(f"Started loading justjoin.it offers.")
        conn = sqlalchemy.create_engine(url=self.db_url)
        self.clean_justjoinit_df.to_sql(name='offers', con=conn, if_exists='append', index=False)
        logging.info(f"Loaded {len(self.clean_justjoinit_df)} justjoin.it offers to a database.")

    def run(self):
        self.__extract()
        self.__transform()
        self.__load()


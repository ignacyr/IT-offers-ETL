import logging
import psycopg2
import sqlalchemy
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import datetime
import ast

first_page_url = "https://nofluffjobs.com/pl/backend?criteria=category%3Dfrontend,fullstack,mobile,testing," \
                 "devops,embedded,security,gaming,artificial-intelligence,big-data,support,it-administrator," \
                 "agile,product-management,project-manager,business-intelligence,business-analyst,ux,sales," \
                 "marketing,backoffice,hr,other&page=1"
passwd_path = "/home/ignacyr/Desktop/password_app_user"


class ETLnofluffjobs:
    def __init__(self, first_url: str, password_path: str, test_pages=1, is_that_test=True):
        self.test = is_that_test
        self.test_pages = test_pages
        self.url = first_url
        self.password_path = password_path

    def __extract(self):
        logging.info(f"Started extracting nofluffjobs.com offers.")
        page = requests.get(self.url)

        soup = BeautifulSoup(page.text, "lxml")

        offers_df = pd.DataFrame(columns=["title", "salary", "level", "skills", "category", "company", "link"])

        counter = 1
        while True:
            postings = soup.find_all("a", {"class": re.compile("posting-list-item")})
            for post in postings:
                try:
                    print(f"Offer {len(offers_df)}.")
                    link = "https://nofluffjobs.com/" + post.get("href")
                    offer_page = requests.get(link)
                    offer_soup = BeautifulSoup(offer_page.text, "lxml")
                    title = offer_soup.find("h1").text
                    salary = offer_soup.find("h4").text
                    level = offer_soup.find("span", {"class": "mr-10 font-weight-medium"}).text
                    skills = offer_soup.find("h3").find_all("a")
                    skills_list = []
                    for s in skills:
                        skills_list.append(s.text)
                    category = offer_soup.find("common-posting-cat-tech").find_all("span")[1].text
                    company = offer_soup.find("a", {"id": "postingCompanyUrl"}).text
                    offers_df.loc[len(offers_df)] = [title, salary, level, skills_list, category, company, link]
                except AttributeError as error:
                    print(error)

            if self.test and counter == self.test_pages:  # for testing read only first page
                break
            counter += 1

            try:
                url = "https://nofluffjobs.com/" + soup.find("a", {"aria-label": "Next"}).get("href")
                page = requests.get(url)
                soup = BeautifulSoup(page.text, "lxml")
            except AttributeError:
                break  # if last page then break

        date = datetime.datetime.now()
        date_str = date.strftime('%Y') + date.strftime('%m') + date.strftime('%d')
        date_int = int(date_str)
        if self.test:
            offers_df['date'] = 99999999
        else:
            offers_df['date'] = date_int

        self.raw_nofluffjobs_df = offers_df

        logging.info(f"Finished extracting {len(offers_df)} nofluffjobs.com offers.")

    def __transform(self):
        logging.info(f"Started transforming nofluffjobs.com offers.")
        nofluffjobs_df = self.raw_nofluffjobs_df

        nofluffjobs_df[["min_salary", "max_salary"]] = nofluffjobs_df['salary'].str.split('-', expand=True)

        nofluffjobs_df['min_salary'] = nofluffjobs_df['min_salary'].astype('str').str.extractall(
            '(\d+)').unstack().fillna('').sum(axis=1).astype(int)
        nofluffjobs_df['max_salary'] = nofluffjobs_df['max_salary'].astype('str').str.extractall(
            '(\d+)').unstack().fillna('').sum(axis=1).astype(int)

        for i in range(len(nofluffjobs_df)):
            if pd.isna(nofluffjobs_df['max_salary'][i]):
                nofluffjobs_df.loc[i, 'max_salary'] = nofluffjobs_df['min_salary'][i]
            if pd.isna(nofluffjobs_df['min_salary'][i]):
                nofluffjobs_df.loc[i, 'min_salary'] = '0'
                nofluffjobs_df.loc[i, 'max_salary'] = '0'
        del i

        nofluffjobs_df = nofluffjobs_df.drop(columns='salary')

        nofluffjobs_df['level'] = nofluffjobs_df['level'].str.split(', ', expand=False)
        nofluffjobs_df['category'] = nofluffjobs_df['category'].str.split(', ', expand=False)

        def cleansing_titles(column: str):
            for a in range(len(nofluffjobs_df)):
                nofluffjobs_df[column][a] = nofluffjobs_df[column][a].strip()  # .casefold()
                # nofluffjobs_df[column][a] = nofluffjobs_df[column][a].replace('.', '_').replace('+', 'p')

        cleansing_titles('title')
        cleansing_titles('company')

        def cleansing_str_list(column: str):
            for a in range(len(nofluffjobs_df)):
                if type(nofluffjobs_df[column][a]) is str:
                    nofluffjobs_df[column][a] = ast.literal_eval(nofluffjobs_df[column][a])
                for b in range(len(nofluffjobs_df[column][a])):
                    nofluffjobs_df[column][a][b] = nofluffjobs_df[column][a][b].strip().casefold()
                for b in range(len(nofluffjobs_df[column][a])):
                    if '&' in nofluffjobs_df[column][a][b]:
                        concatenated = ','.join(nofluffjobs_df[column][a])
                        nofluffjobs_df[column][a] = re.split(',| & ', concatenated)
                    if nofluffjobs_df[column][a][b].lower() == 'inne':
                        nofluffjobs_df[column][a][b] = 'other'
                    nofluffjobs_df[column][a][b] = nofluffjobs_df[column][a][b].replace('"', '')

        cleansing_str_list('level')
        cleansing_str_list('category')
        cleansing_str_list('skills')

        nofluffjobs_df['min_salary'] = nofluffjobs_df['min_salary'].astype(int)
        nofluffjobs_df['max_salary'] = nofluffjobs_df['max_salary'].astype(int)

        nofluffjobs_df['level'] = nofluffjobs_df['level'].astype(str)
        nofluffjobs_df['category'] = nofluffjobs_df['category'].astype(str)
        nofluffjobs_df['skills'] = nofluffjobs_df['skills'].astype(str)

        self.clean_nofluffjobs_df = nofluffjobs_df
        logging.info(f"Finished transforming nofluffjobs.com offers.")

    def __load(self):
        logging.info(f"Started loading nofluffjobs.com offers.")

        def column_load_log(col):
            date = datetime.datetime.now()
            column_log = f"{date.strftime('%x')} - {date.strftime('%X')}: " \
                         f"Postgres: Added {col} column.++++++"
            print(column_log)
            with open("load.log", "a") as file:
                file.write(column_log + '\n')

        with open(self.password_path) as f:
            password_app_user = f.readline().strip()

        conn = sqlalchemy.create_engine(
            f'postgresql://app_user:{password_app_user}@it-offers.c9umk0ew1r8h.us-east-1.rds.amazonaws.com:5432/dwh')

        conn.execute("CREATE TABLE IF NOT EXISTS offers (id serial primary key);")

        clean_nofluffjobs_df = self.clean_nofluffjobs_df

        schema_list = list(clean_nofluffjobs_df.columns)

        for column in schema_list:
            try:
                if column in ("min_salary", "max_salary", "date"):
                    conn.execute(f"ALTER TABLE offers ADD COLUMN {column} integer;")
                elif column in ("title", "level", "skills", "category", "company", "link"):
                    conn.execute(f"ALTER TABLE offers ADD COLUMN {column} varchar;")
                else:
                    conn.execute(f"ALTER TABLE offers ADD COLUMN {column} boolean;")
                column_load_log(column)
            except psycopg2.OperationalError as error:
                print(f"Postgres: {column} is already in a table.########################")
            except sqlalchemy.exc.ProgrammingError as error:
                print(f"Postgres: {column} is already in a table.########################")

        clean_nofluffjobs_df.to_sql(name='offers', con=conn, if_exists='append', index=False)

        logging.info(f"Loaded {len(clean_nofluffjobs_df)} nofluffjobs.com offers to a database.")

    def run(self):
        self.__extract()
        self.__transform()
        self.__load()


if __name__ == "__main__":
    logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s:%(levelname)s: %(message)s')
    etl_job = ETLnofluffjobs(first_url=first_page_url, password_path=passwd_path, test_pages=1, is_that_test=True)
    etl_job.run()

from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import time
import datetime

only_first_pages_for_test = True
pages = 1


def run():
    # url = "https://nofluffjobs.com/pl/backend?criteria=category%3Dfrontend,fullstack,mobile,testing,devops," \
    #       "embedded,security,gaming,artificial-intelligence,big-data,support,it-administrator,agile," \
    #       "product-management,project-manager,business-intelligence,business-analyst,ux,sales,marketing," \
    #       "backoffice,hr,other&page=1"

    url = "https://nofluffjobs.com/pl/backend?criteria=category%3Dfrontend,fullstack,mobile,testing,devops,embedded,security,gaming,artificial-intelligence,big-data,support,it-administrator,agile,product-management,project-manager,business-intelligence,business-analyst,ux,sales,marketing,backoffice,hr,other&page=1"

    page = requests.get(url)

    soup = BeautifulSoup(page.text, "lxml")

    offers_df = pd.DataFrame(columns=["title", "salary", "level", "skills", "category", "company", "link"])

    start = time.time()
    counter = 1
    while True:
        postings = soup.find_all("a", {"class": re.compile("posting-list-item")})
        for post in postings:
            try:
                print(f"Offer {len(offers_df)}.")
                link = "https://nofluffjobs.com/" + post.get("href")
                print(link)
                offer_page = requests.get(link)
                offer_soup = BeautifulSoup(offer_page.text, "lxml")
                title = offer_soup.find("h1").text
                print(title)
                salary = offer_soup.find("h4").text
                print(salary)
                level = offer_soup.find("span", {"class": "mr-10 font-weight-medium"}).text
                print(level)
                skills = offer_soup.find("h3").find_all("a")
                skills_list = []
                for s in skills:
                    skills_list.append(s.text)
                    print(s.text)
                category = offer_soup.find("common-posting-cat-tech").find_all("span")[1].text
                print(category)
                company = offer_soup.find("a", {"id": "postingCompanyUrl"}).text
                print(company)
                offers_df.loc[len(offers_df)] = [title, salary, level, skills_list, category, company, link]
            except AttributeError as error:
                print(error)

        if only_first_pages_for_test and counter == pages:  # for testing read only first page
            break
        counter += 1

        try:
            url = "https://nofluffjobs.com/" + soup.find("a", {"aria-label": "Next"}).get("href")
            page = requests.get(url)
            soup = BeautifulSoup(page.text, "lxml")
        except AttributeError:
            break  # if last page then break

    date = datetime.datetime.now()
    date_str = date.strftime('%Y')+date.strftime('%m')+date.strftime('%d')
    date_int = int(date_str)
    offers_df['date'] = date_int
    print(offers_df)
    offers_df.to_csv("staging.csv")

    end = time.time()

    extract_log = f"{date.strftime('%x')} - {date.strftime('%X')}: " \
                  f"Finished scraping {len(offers_df)} offers " \
                  f"in {datetime.timedelta(seconds=(round(end - start)))} [hh:mm:ss]"
    print(extract_log)
    with open("extract.log", "a") as f:
        f.write(extract_log + '\n')


if __name__ == '__main__':
    run()

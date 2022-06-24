from etl_nofluffjobs import ETLnofluffjobs
import db_config


def main():
    first_page_url = "https://nofluffjobs.com/pl/backend?criteria=category%3Dfrontend,fullstack,mobile,testing," \
                     "devops,embedded,security,gaming,artificial-intelligence,big-data,support,it-administrator," \
                     "agile,product-management,project-manager,business-intelligence,business-analyst,ux,sales," \
                     "marketing,backoffice,hr,other&page=1"

    passwd_path = "/home/ignacyr/Desktop/password_app_user"
    with open(passwd_path) as file:
        password = file.readline().strip()
    db_url = f"{db_config.DB}://{db_config.USERNAME}:{password}@{db_config.HOST}"

    etl_nofluffjobs_job = ETLnofluffjobs(first_url=first_page_url, password_path=passwd_path,
                                         db_url=db_url,
                                         test_pages=1, is_that_test=True)

    etl_nofluffjobs_job.run()


if __name__ == "__main__":
    main()


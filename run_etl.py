from ETL import extract, transform, load
import db_backup


def main():
    extract.run()
    transform.run()
    load.run()
    db_backup.create()


if __name__ == '__main__':
    main()

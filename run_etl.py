from ETL import extract, transform, load, load_pg
import db_backup


def main():
    extract.run()
    transform.run()
    load.run()
    db_backup.create()
    load_pg.run()


if __name__ == '__main__':
    main()

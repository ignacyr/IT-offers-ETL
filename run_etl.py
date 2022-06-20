from ETL import extract, transform, load, load_pg
import db_backup


def main():
    # extract.run()
    # transform.run()
    # load.run()
    load_pg.run()
    # db_backup.create()


if __name__ == '__main__':
    main()

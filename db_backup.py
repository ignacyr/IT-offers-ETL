import sqlite3


def progress(status, remaining, total):
    print(f'Copied {total - remaining} of {total} pages...')


def create():
    try:
        # existing DB
        sqliteCon = sqlite3.connect('dwh.db')
        # copy into this DB
        backupCon = sqlite3.connect('dwh_backup.db')
        with backupCon:
            sqliteCon.backup(backupCon, pages=1, progress=progress)
        print("backup successful")
    except sqlite3.Error as error:
        print("Error while taking backup: ", error)
    finally:
        if backupCon:
            backupCon.close()
            sqliteCon.close()


if __name__ == "__main__":
    create()


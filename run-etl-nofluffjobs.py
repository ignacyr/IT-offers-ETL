from etl_nofluffjobs import extract, transform, load


def main():
    extract.run()
    transform.run()
    load.run()


if __name__ == '__main__':
    main()

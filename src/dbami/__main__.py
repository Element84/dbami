from typing import NoReturn

from dbami.cli import get_cli


def main(argv=None) -> NoReturn:
    cli = get_cli()
    cli(argv)


if __name__ == "__main__":
    main()

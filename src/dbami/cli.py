import abc
import argparse
import os
import sys
from pathlib import Path
from typing import Any, Callable, Literal, NoReturn, Optional, Sequence, Union

import asyncpg

from dbami import exceptions
from dbami.constants import SCHEMA_VERSION_TABLE
from dbami.db import DB
from dbami.util import syncrun


def printe(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def arg_wait_timeout(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--wait-timeout",
        action=EnvDefault,
        envvars=("DBAMI_WAIT_TIMEOUT",),
        default=60,
        type=int,
        help="seconds to wait for db connection",
    )


def arg_project(
    parser: argparse.ArgumentParser,
    type: Callable = lambda x: DB(Path(x)),
) -> None:
    parser.add_argument(
        "--project-directory",
        action=EnvDefault,
        envvars=("DBAMI_PROJECT_DIRECTORY",),
        default=Path.cwd(),
        type=type,
    )


def arg_database(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-d",
        "--database",
        action=EnvDefault,
        envvars=["PGDATABASE"],
        metavar="DATABASE_NAME",
    )


def arg_version_table(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--schema-version-table",
        action=EnvDefault,
        envvars=["DBAMI_SCHEMA_VERSION_TABLE"],
        default=SCHEMA_VERSION_TABLE,
        help=(
            "name of the table (optionally schema-qualified) "
            "in which to store applied schema versions"
        ),
    )


def target_type(val: Any, default_label: str) -> int:
    if val == default_label:
        return -1
    else:
        val = int(val)

    if val < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")

    return val


def arg_migration_target(
    parser: argparse.ArgumentParser,
    default_label: Union[Literal["latest"], Literal["last"]],
) -> None:
    parser.add_argument(
        "--target",
        metavar="TARGET_MIGRATION_ID",
        default=default_label,
        type=lambda x: target_type(x, default_label),
        help=f"(default: '{default_label}')",
    )


def arg_pg_dump_exec(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--pg-dump",
        action=EnvDefault,
        envvars=["DBAMI_PG_DUMP"],
        default="pg_dump",
        help=("path to pg_dump executable or name to lookup on path"),
    )


class EnvDefault(argparse.Action):
    def __init__(
        self,
        envvars: Sequence[str],
        required: bool = True,
        default: Optional[Any] = None,
        help: str = "",
        supress_help_modification: bool = False,
        **kwargs,
    ) -> None:
        # required is set to True by default so argparse
        # will fail if the parameter is not set via an option
        # or via an env var.

        # but if we have a default then the parameter
        # cannot be required
        if default:
            required = False

        if not supress_help_modification:
            extra: list[str] = []

            if required:
                extra.append("required")

            if default:
                extra.append(f"default: '{default}'")

            envstr = ",".join([f"${v}" for v in envvars])
            extra.append(f"env: {envstr}")

            newline = "\n" if help else ""

            help += f"{newline}({'; '.join(extra)})"

        for envvar in envvars:
            envval = os.getenv(envvar)

            if envval is not None:
                default = envval
                # if we get a val from the env then we don't
                # want argparse to fail if the option isn't
                # specified
                required = False
                break

        if default:
            required = False

        super().__init__(
            default=default,
            required=required,
            help=help,
            **kwargs,
        )

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values,
        option_string: Optional[str] = None,
    ) -> None:
        setattr(namespace, self.dest, values)


class Command(abc.ABC):
    help: str = ""

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        pass

    def process_args(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace
    ) -> None:
        pass

    @abc.abstractmethod
    def __call__(self, args: argparse.Namespace) -> int:
        pass


class CLI:
    def __init__(self, prog: str, description: str) -> None:
        self.parser = argparse.ArgumentParser(
            prog=prog,
            description=description,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        self._subparsers = self.parser.add_subparsers(
            title="subcommands",
            dest="subcommand",
        )
        self._subparsers.metavar = "{command}"

    def add_command(self, command: Command) -> None:
        name = getattr(command, "name", command.__class__.__name__.lower())
        parser = self._subparsers.add_parser(
            name,
            help=getattr(command, "help", None),
            aliases=getattr(command, "aliases", []),
        )
        command.set_args(parser)
        parser.set_defaults(_cmd=command)

    def __call__(self, argv: Optional[Sequence[str]] = None) -> NoReturn:
        args: argparse.Namespace = self.parser.parse_args(argv)

        if args.subcommand is None:
            printe("error: subcommand required")
            self.parser.print_help()
            sys.exit(2)

        args._cmd.process_args(self.parser, args)
        sys.exit(args._cmd(args))


class Init(Command):
    help: str = "Initialize a new dbami project in the current directory"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_project(parser, type=Path)

    def __call__(self, args: argparse.Namespace) -> int:
        DB.new_project(args.project_directory)
        return 0


class New(Command):
    help: str = "Create a new migration with the given name"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_project(parser)
        parser.add_argument("migration_name")

    def __call__(self, args: argparse.Namespace) -> int:
        DB(args.project_directory).new_migration(args.migration_name)
        return 0


class Create(Command):
    help: str = "Create a database"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_wait_timeout(parser)
        arg_database(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        syncrun(DB.create_database(args.database))
        return 0


class Drop(Command):
    help: str = "Drop a database"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_wait_timeout(parser)
        arg_database(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        syncrun(DB.drop_database(args.database))
        return 0


class Pending(Command):
    help: str = "List all unapplied migrations"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_wait_timeout(parser)
        arg_project(parser)
        arg_database(parser)
        arg_version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            db = DB(args.project_directory)

            async for migration in db.yield_unapplied_migrations(
                database=args.database
            ):
                print(f"{migration.id} {migration.name}")
            return 0

        return syncrun(run())


class CurrentSchema(Command):
    help: str = "Get current schema version"
    name: str = "current-schema"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_wait_timeout(parser)
        arg_project(parser)
        arg_database(parser)
        arg_version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            db = DB(args.project_directory)
            print(await db.get_current_version(database=args.database))
            return 0

        return syncrun(run())


class LoadSchema(Command):
    help: str = "Load the schema.sql into a database"
    name: str = "load-schema"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_project(parser)
        arg_wait_timeout(parser)
        arg_database(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            await DB(args.project_directory).load_schema(database=args.database)
            return 0

        return syncrun(run())


class Migrate(Command):
    help: str = "Migrate the database to the latest (or specified) version"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_wait_timeout(parser)
        arg_project(parser)
        arg_database(parser)
        arg_migration_target(parser, "latest")
        arg_version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            db = DB(args.project_directory)

            target = None if args.target == -1 else args.target
            try:
                await db.migrate(target=target, direction="up", database=args.database)
            except exceptions.DirectionError as e:
                printe(e)
                return 1
            return 0

        return syncrun(run())


class Rollback(Command):
    help: str = "Rollback the database to the last (or specified) version"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_wait_timeout(parser)
        arg_project(parser)
        arg_database(parser)
        arg_migration_target(parser, "last")
        arg_version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            db = DB(args.project_directory)

            target: int = args.target
            if target == -1:
                current = await db.get_current_version(database=args.database)

                if current is None:
                    printe("Cannot rollback: database has no applied schema version")
                    return 1

                target = current - 1

            try:
                await db.migrate(
                    target=target, direction="down", database=args.database
                )
            except exceptions.DirectionError as e:
                printe(e)
                return 1
            return 0

        return syncrun(run())


class Up(Command):
    help: str = "Migrate to the latest version, creating the database if necessary"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_wait_timeout(parser)
        arg_project(parser)
        arg_database(parser)
        arg_version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            db = DB(args.project_directory)
            try:
                await db.create_database(args.database)
            except asyncpg.DuplicateDatabaseError:
                pass

            try:
                await db.migrate(direction="up", database=args.database)
            except exceptions.DirectionError as e:
                printe(e)
                return 1
            return 0

        return syncrun(run())


class Verify(Command):
    help: str = "Check that the schema and migrations are in sync"

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        arg_wait_timeout(parser)
        arg_project(parser)
        arg_version_table(parser)
        arg_pg_dump_exec(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            db = DB(args.project_directory)
            return int(not await db.verify(_pg_dump=args.pg_dump))

        return syncrun(run())


class Version(Command):
    help: str = "Print the dbami version"

    def __call__(self, args: argparse.Namespace) -> int:
        from dbami.version import __version__

        print(f"dbami version: {__version__}")
        return 0


def get_cli() -> CLI:
    cli = CLI(
        prog="dbami",
        description="The database friend you didn't know you needed.",
    )
    cli.add_command(Init())
    cli.add_command(New())
    cli.add_command(Create())
    cli.add_command(Drop())
    cli.add_command(Pending())
    cli.add_command(CurrentSchema())
    cli.add_command(LoadSchema())
    cli.add_command(Migrate())
    cli.add_command(Rollback())
    cli.add_command(Up())
    cli.add_command(Verify())
    cli.add_command(Version())

    return cli

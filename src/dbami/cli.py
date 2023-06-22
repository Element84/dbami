import abc
import argparse
import os
import sys
from pathlib import Path
from typing import Any, Literal, NoReturn, Optional, Sequence, Union

import asyncpg

from dbami import exceptions
from dbami.constants import SCHEMA_VERSION_TABLE
from dbami.db import DB
from dbami.util import syncrun

DEFAULT_ENV_PREFIX = "DBAMI"


def printe(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


def target_type(val: Any, default_label: str) -> int:
    if val == default_label:
        return -1
    else:
        val = int(val)

    if val < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")

    return val


class Arguments:
    env_prefix: str = DEFAULT_ENV_PREFIX

    @classmethod
    def wait_timeout(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--wait-timeout",
            action=EnvDefault,
            envvars=(f"{cls.env_prefix}_WAIT_TIMEOUT",),
            default=60,
            type=int,
            help="seconds to wait for db connection",
        )

    @classmethod
    def project(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--project-directory",
            action=EnvDefault,
            envvars=(f"{cls.env_prefix}_PROJECT_DIRECTORY",),
            default=Path.cwd(),
            type=Path,
        )

    @classmethod
    def database(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "-d",
            "--database",
            action=EnvDefault,
            envvars=["PGDATABASE"],
            metavar="DATABASE_NAME",
        )

    @classmethod
    def version_table(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--schema-version-table",
            action=EnvDefault,
            envvars=[f"{cls.env_prefix}_SCHEMA_VERSION_TABLE"],
            default=SCHEMA_VERSION_TABLE,
            help=(
                "name of the table (optionally schema-qualified) "
                "in which to store applied schema versions"
            ),
        )

    @classmethod
    def migration_target(
        cls,
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

    @classmethod
    def pg_dump_exec(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--pg-dump",
            action=EnvDefault,
            envvars=[f"{cls.env_prefix}_PG_DUMP"],
            default="pg_dump",
            help=("path to pg_dump executable or name to lookup on path"),
        )

    @classmethod
    def fixture_path(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--fixture-dir",
            type=Path,
            action="append",
            default=[],
            dest="fixture_dirs",
            help=(
                "directory from which to load sql fixtures; "
                "later directories take precedence"
            ),
        )

    @classmethod
    def process_project(
        cls,
        parser: argparse.ArgumentParser,
        args: argparse.Namespace,
    ) -> None:
        project: Optional[Path] = getattr(args, "project_directory", None)
        svt: str = getattr(
            args,
            "schema_version_table",
            SCHEMA_VERSION_TABLE,
        )
        fixture_dirs: list[Path] = getattr(args, "fixture_dirs", [])

        if project is None:
            args.db = DB
            return

        try:
            args.db = DB(project, schema_version_table=svt)
        except FileNotFoundError:
            parser.error(
                f"Project directory does not appear to be valid: '{project}'",
            )

        for fixture_dir in fixture_dirs:
            args.db.add_fixture_dir(fixture_dir)


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

    @property
    def name(self) -> str:
        return self.__class__.__name__.lower()

    def set_args(self, parser: argparse.ArgumentParser) -> None:
        pass

    def process_args(
        self,
        parser: argparse.ArgumentParser,
        args: argparse.Namespace,
    ) -> None:
        pass

    @abc.abstractmethod
    def __call__(self, args: argparse.Namespace) -> int:
        pass


class DbamiCommand(Command):
    def process_args(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace
    ) -> None:
        Arguments.process_project(parser, args)


class Init(DbamiCommand):
    help: str = (
        "Initialize a new dbami project (in the current directory unless specified)"
    )

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)

    def process_args(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace
    ) -> None:
        args.db = DB

    def __call__(self, args: argparse.Namespace) -> int:
        args.db.new_project(args.project_directory)
        return 0


class New(DbamiCommand):
    help: str = "Create a new migration with the given name"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        parser.add_argument("migration_name")

    def __call__(self, args: argparse.Namespace) -> int:
        args.db.new_migration(args.migration_name)
        return 0


class Create(DbamiCommand):
    help: str = "Create a database"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.wait_timeout(parser)
        Arguments.database(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        syncrun(args.db.create_database(args.database))
        return 0


class Drop(DbamiCommand):
    help: str = "Drop a database"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.wait_timeout(parser)
        Arguments.database(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        syncrun(args.db.drop_database(args.database))
        return 0


class Pending(DbamiCommand):
    help: str = "List all unapplied migrations"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.wait_timeout(parser)
        Arguments.database(parser)
        Arguments.version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            async for migration in args.db.yield_unapplied_migrations(
                database=args.database
            ):
                print(f"{migration.id} {migration.name}")
            return 0

        return syncrun(run())


class CurrentSchema(DbamiCommand):
    help: str = "Get current schema version"
    name: str = "current-schema"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.wait_timeout(parser)
        Arguments.database(parser)
        Arguments.version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            print(await args.db.get_current_version(database=args.database))
            return 0

        return syncrun(run())


class LoadSchema(DbamiCommand):
    help: str = "Load the schema.sql into a database"
    name: str = "load-schema"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.wait_timeout(parser)
        Arguments.database(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            await args.db.load_schema(database=args.database)
            return 0

        return syncrun(run())


class Migrate(DbamiCommand):
    help: str = "Migrate the database to the latest (or specified) version"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.wait_timeout(parser)
        Arguments.database(parser)
        Arguments.migration_target(parser, "latest")
        Arguments.version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            target: Optional[int] = None if args.target == -1 else args.target
            try:
                await args.db.migrate(
                    target=target,
                    direction="up",
                    database=args.database,
                )
            except exceptions.DirectionError as e:
                printe(e)
                return 1
            return 0

        return syncrun(run())


class Rollback(DbamiCommand):
    help: str = "Rollback the database to the last (or specified) version"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.wait_timeout(parser)
        Arguments.database(parser)
        Arguments.migration_target(parser, "last")
        Arguments.version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            target: int = args.target
            if target == -1:
                current: Optional[int] = await args.db.get_current_version(
                    database=args.database,
                )

                if current is None:
                    printe("Cannot rollback: database has no applied schema version")
                    return 1

                target = current - 1

            try:
                await args.db.migrate(
                    target=target,
                    direction="down",
                    database=args.database,
                )
            except (exceptions.DirectionError, exceptions.MigrationError) as e:
                printe(e)
                return 1
            return 0

        return syncrun(run())


class Up(DbamiCommand):
    help: str = "Migrate to the latest version, creating the database if necessary"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.wait_timeout(parser)
        Arguments.database(parser)
        Arguments.version_table(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            try:
                await args.db.create_database(args.database)
            except asyncpg.DuplicateDatabaseError:
                pass

            try:
                await args.db.migrate(direction="up", database=args.database)
            except (exceptions.DirectionError, exceptions.MigrationError) as e:
                printe(e)
                return 1
            return 0

        return syncrun(run())


class Verify(DbamiCommand):
    help: str = "Check that the schema and migrations are in sync"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.wait_timeout(parser)
        Arguments.version_table(parser)
        Arguments.pg_dump_exec(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            return int(not await args.db.verify(pg_dump=args.pg_dump))

        return syncrun(run())


class Version(DbamiCommand):
    help: str = "Print the cli version"

    def __call__(self, args: argparse.Namespace) -> int:
        from dbami.version import __version__

        print(f"dbami version: {__version__}")
        return 0


class ListFixtures(DbamiCommand):
    help: str = "List all available fixture files on search path"
    name: str = "list-fixtures"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.fixture_path(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        for name, sqlfile in args.db.fixtures.items():
            print(f"{name} ({sqlfile.path})")
        return 0


class LoadFixture(DbamiCommand):
    help: str = "Load a sql fixture into the database"
    name: str = "load-fixture"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.fixture_path(parser)
        Arguments.wait_timeout(parser)
        Arguments.database(parser)
        parser.add_argument("fixture_name", help="name of fixture to load")

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            await args.db.load_fixture(args.fixture_name, database=args.database)
            return 0

        return syncrun(run())


class ExecuteSql(DbamiCommand):
    help: str = "Run SQL from stdin against the database"
    name: str = "execute-sql"

    def set_args(
        self,
        parser: argparse.ArgumentParser,
    ) -> None:
        Arguments.project(parser)
        Arguments.wait_timeout(parser)
        Arguments.database(parser)

    def __call__(self, args: argparse.Namespace) -> int:
        async def run() -> int:
            await args.db.execute_sql(sys.stdin.read(), database=args.database)
            return 0

        return syncrun(run())


class CLI(abc.ABC):
    def __init__(
        self,
        prog: str,
        description: str,
    ) -> None:
        self.parser = argparse.ArgumentParser(
            prog=prog,
            description=description,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        self._subparsers = self.parser.add_subparsers(
            title="commands",
            dest="command",
        )
        self._subparsers.metavar = "[command]"

    def add_command(self, command: Command) -> None:
        parser = self._subparsers.add_parser(
            command.name,
            help=getattr(command, "help", None),
            aliases=getattr(command, "aliases", []),
        )
        command.set_args(parser)
        parser.set_defaults(_cmd=command)

    def _process_args(
        self,
        argv: Optional[Sequence[str]] = None,
    ) -> argparse.Namespace:
        args: argparse.Namespace = self.parser.parse_args(argv)

        if args.command is None:
            printe("error: command required")
            self.parser.print_help()
            sys.exit(2)

        args._cmd.process_args(self.parser, args)
        return args

    def process_args(self, args: argparse.Namespace) -> None:
        pass

    def __call__(self, argv: Optional[Sequence[str]] = None) -> NoReturn:
        args = self._process_args(argv)
        sys.exit(args._cmd(args))


class DbamiCLI(CLI):
    commands: dict[str, DbamiCommand] = {
        cmd.name: cmd
        for cmd in (
            Init(),
            New(),
            Create(),
            Drop(),
            Pending(),
            CurrentSchema(),
            LoadSchema(),
            Migrate(),
            Rollback(),
            Up(),
            Verify(),
            Version(),
            ListFixtures(),
            LoadFixture(),
            ExecuteSql(),
        )
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for cmd in DbamiCLI.commands.values():
            self.add_command(cmd)


def get_cli() -> DbamiCLI:
    return DbamiCLI(
        prog="dbami",
        description="The database friend you didn't know you needed.",
    )

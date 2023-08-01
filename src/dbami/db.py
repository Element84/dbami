import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Literal, Optional, TextIO, Union

import asyncpg
from buildpg import V, render

from dbami import exceptions
from dbami.constants import SCHEMA_VERSION_TABLE
from dbami.util import random_name

logger = logging.getLogger(__name__)


class SqlFile:
    def __init__(self, path: Path):
        self.name = path.stem
        self.path = path

    def __lt__(self, other) -> bool:
        return self.name < other.name

    def __gt__(self, other) -> bool:
        return self.name > other.name

    def __le__(self, other) -> bool:
        return self.name <= other.name

    def __ge__(self, other) -> bool:
        return self.name >= other.name

    def __eq__(self, other) -> bool:
        return self.name == other.name

    def __ne__(self, other) -> bool:
        return self.name != other.name


class Migration:
    def __init__(
        self,
        id: int,
        name: str,
        up: SqlFile,
        down: Optional[SqlFile] = None,
        parent: Optional["Migration"] = None,
        child: Optional["Migration"] = None,
    ):
        self.id = id
        self.name = name
        self.up = up
        self.down = down
        self.parent = parent
        self.child = child

        if parent:
            parent.child = self

        if child:
            child.parent = self

    @classmethod
    def from_up_path(
        cls,
        up_path: Path,
        parent: Optional["Migration"] = None,
        child: Optional["Migration"] = None,
    ) -> "Migration":
        try:
            full_name = up_path.name.replace(".up.sql", "")
            __id, name = full_name.split("_", maxsplit=1)
            _id = int(__id)
        except Exception:
            raise ValueError(
                f"Cannot extract migration ID and/or name from path '{up_path}'"
            )

        down = None
        down_path = up_path.with_name(f"{full_name}.down.sql")
        if down_path.is_file():
            down = SqlFile(down_path)

        return cls(_id, name, SqlFile(up_path), down, parent=parent, child=child)

    def __lt__(self, other) -> bool:
        return self.id < other.id

    def __gt__(self, other) -> bool:
        return self.id > other.id

    def __le__(self, other) -> bool:
        return self.id <= other.id

    def __ge__(self, other) -> bool:
        return self.id >= other.id

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def __ne__(self, other) -> bool:
        return self.id != other.id

    def __str__(self) -> str:
        return (
            f"<dbami.db.Migration id={self.id} name={self.name} "
            f"path={self.up.path} has_down={self.down is not None}>"
        )


def migrations_from_dir(directory: Path) -> dict[int, Migration]:
    parent: Optional[Migration] = None
    migrations: dict[int, Migration] = {}

    for migration_path in sorted(directory.glob("*.up.sql")):
        migration = Migration.from_up_path(migration_path)
        migrations[migration.id] = migration

    for _, migration in sorted(migrations.items()):
        if parent:
            parent.child = migration
            migration.parent = parent
        parent = migration

    return migrations


def find_next_migration(
    current: int, migrations: dict[int, Migration]
) -> Optional[Migration]:
    if not migrations:
        return None

    try:
        # current migration known, return its child, if one
        return migrations[current].child
    except KeyError:
        pass

    # if current migration greater than max migration then none apply
    if current >= max(migrations.keys()):
        return None

    # if current migration version one less than min migration then all apply
    min_id = min(migrations.keys())
    if current == min_id - 1:
        return migrations[min_id]

    # if we got her we don't have a valid migration path :(
    raise exceptions.MigrationError(f"No migration path from schema version {current}")


class DB:
    def __init__(
        self,
        project: Path,
        schema_version_table: str = SCHEMA_VERSION_TABLE,
    ) -> None:
        self.project_dir = project
        self.validate_project()
        self.schema = SqlFile(self.schema_file)
        self.migrations = migrations_from_dir(self.migrations_dir)

        if not self.migrations:
            raise FileNotFoundError(
                "Project is missing base migration file. Try reinitializing."
            )

        self.fixtures = self.load_fixtures_from_dir(self.fixtures_dir)
        self.schema_version_table = schema_version_table

    @classmethod
    def project_schema(cls, project: Path):
        return project.joinpath("schema.sql")

    @classmethod
    def project_migrations(cls, project: Path):
        return project.joinpath("migrations")

    @classmethod
    def project_fixtures(cls, project: Path):
        return project.joinpath("fixtures")

    @classmethod
    def new_project(cls, directory: Path):
        cls.project_schema(directory).touch()
        migrations = cls.project_migrations(directory)
        migrations.mkdir(exist_ok=True)
        migrations.joinpath("00000_base.up.sql").touch()
        cls.project_fixtures(directory).mkdir(exist_ok=True)
        return cls(directory)

    @property
    def schema_file(self):
        return self.project_schema(self.project_dir)

    @property
    def migrations_dir(self):
        return self.project_migrations(self.project_dir)

    @property
    def fixtures_dir(self):
        return self.project_fixtures(self.project_dir)

    @staticmethod
    def load_fixtures_from_dir(directory: Path) -> dict[str, SqlFile]:
        return {f.name: f for f in (SqlFile(f) for f in directory.glob("*.sql"))}

    def add_fixture_dir(self, directory: Path) -> None:
        self.fixtures.update(self.load_fixtures_from_dir(directory))

    def validate_project(self):
        schema = self.schema_file
        migrations = self.migrations_dir
        fixtures = self.fixtures_dir

        if not schema.is_file():
            raise FileNotFoundError(
                f"Schema does not exist or is wrong type: {schema}",
            )

        if not migrations.is_dir():
            raise FileNotFoundError(
                f"Migrations directory does not exist or is wrong type: {migrations}",
            )

        if fixtures.exists() and not fixtures.is_dir():
            raise ValueError(
                f"Fixtures directory is not a directory: {migrations}",
            )

    def next_migration_id(self):
        return max(self.migrations.keys()) + 1 if self.migrations else 0

    def new_migration(
        self,
        name: str,
        up_content: str = "",
        down_content: Optional[str] = "",
    ):
        _id = self.next_migration_id()
        base = self.migrations_dir.joinpath(f"{str(_id).zfill(5)}_{name}")
        up = base.with_suffix(".up.sql")
        down = base.with_suffix(".down.sql")
        up.touch()
        down.touch()

        if up_content is not None:
            up.write_text(up_content)

        if down_content is not None:
            down.write_text(down_content)

        self.migrations[_id] = Migration(
            _id,
            name,
            SqlFile(up),
            SqlFile(down),
            parent=self.migrations.get(_id - 1),
        )

    def new_fixture(self, name: str, content: str = ""):
        f = self.fixtures_dir.joinpath(f"{name}.sql")

        if f.exists():
            raise FileExistsError("Cannot create fixture, already exists: {f}")

        f.touch()

        if content is not None:
            f.write_text(content)

        self.fixtures[name] = SqlFile(f)

    @staticmethod
    @asynccontextmanager
    async def get_db_connection(**kwargs):
        conn = None
        try:
            if kwargs.get("conn"):
                yield kwargs["conn"]
            else:
                conn = await asyncpg.connect(**kwargs)
                yield conn
        finally:
            if conn:
                await conn.close()

    @classmethod
    async def execute_sql(cls, sql: str, *query_params, **kwargs) -> None:
        if not sql:
            # asyncpg chokes on an empty string,
            # so we bail out early on any non-truthy sql
            return

        async with cls.get_db_connection(**kwargs) as conn:
            await conn.execute(sql, *query_params)

    @classmethod
    async def create_database(cls, db_name: str, **kwargs) -> None:
        kwargs["database"] = ""
        await cls.execute_sql(f'CREATE DATABASE "{db_name}";', **kwargs)

    @classmethod
    async def drop_database(cls, db_name: str, **kwargs) -> None:
        kwargs["database"] = ""
        await cls.execute_sql(f'DROP DATABASE "{db_name}";', **kwargs)

    @classmethod
    async def run_sqlfile(cls, sqlfile: SqlFile, **kwargs) -> None:
        await cls.execute_sql(sqlfile.path.read_text(), **kwargs)

    async def get_current_version(
        self,
        **kwargs,
    ) -> Optional[int]:
        async with self.get_db_connection(**kwargs) as conn:
            query, _ = render(
                """
                SELECT
                    version
                FROM :version_table
                WHERE
                    applied_at = (SELECT max(applied_at) from :version_table)
                """,
                version_table=V(self.schema_version_table),
            )

            try:
                version = await conn.fetchval(query)
            except asyncpg.UndefinedTableError:
                return None

        return version

    async def yield_unapplied_migrations(
        self, **kwargs
    ) -> AsyncGenerator[Migration, None]:
        _version = await self.get_current_version(**kwargs)
        schema_version = _version if _version is not None else -1
        next_migration = find_next_migration(schema_version, self.migrations)

        while next_migration:
            yield next_migration
            next_migration = next_migration.child

    async def load_schema(self, **kwargs) -> None:
        async with self.get_db_connection(**kwargs) as conn:
            async with conn.transaction():
                await self.run_sqlfile(self.schema, conn=conn)
                await self._update_schema_version(
                    max(self.migrations.keys()),
                    conn,
                )

    async def load_fixture(self, fixture_name: str, **kwargs):
        try:
            fixture = self.fixtures[fixture_name]
        except KeyError:
            raise FileNotFoundError(f"Unknown fixture: '{fixture_name}'")

        await self.run_sqlfile(fixture, **kwargs)

    async def _update_schema_version(
        self,
        version: int,
        conn,
    ):
        table_sql, _ = render(
            """
            CREATE TABLE IF NOT EXISTS :version_table (
                version integer,
                applied_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE INDEX ON :version_table (version);
            CREATE INDEX ON :version_table (applied_at);
            """,
            version_table=V(self.schema_version_table),
        )
        version_sql, params = render(
            "INSERT INTO :version_table (version) VALUES (:version)",
            version_table=V(self.schema_version_table),
            version=version,
        )

        try:
            # use a subtransaction (save point) to prevent this
            # "expected" exception from rolling back the migration
            async with conn.transaction():
                await self.execute_sql(table_sql, conn=conn)
        except asyncpg.InvalidSchemaNameError:
            schema = self.schema_version_table.split(".")[0]
            schema_sql, _ = render(
                "CREATE SCHEMA IF NOT EXISTS :schema",
                schema=V(schema),
            )
            await self.execute_sql(schema_sql, conn=conn)
            await self.execute_sql(table_sql, conn=conn)

        await self.execute_sql(version_sql, *params, conn=conn)

    async def migrate(
        self,
        target: Optional[int] = None,
        direction: Union[Literal["up"], Literal["down"], None] = None,
        **kwargs,
    ):
        if not self.migrations:
            return

        min_migration = min(self.migrations.keys())

        async with self.get_db_connection(**kwargs) as conn:
            _version = await self.get_current_version(conn=conn)
            schema_version: int = (
                _version if _version is not None else min_migration - 1
            )

            if target is None:
                target = max(self.migrations.keys())

                # if target was not specified then we never want to roll back
                if schema_version > target:
                    logger.warning(
                        "Current schema version %s greater than all migrations",
                        schema_version,
                    )
                    return

            if schema_version == target:
                return

            if target not in self.migrations:
                if target < min_migration:
                    raise exceptions.MigrationError(
                        f"Target migration ID '{target}' would cause unsupported "
                        f"rollback of base migration ID '{min_migration}'"
                    )

                raise exceptions.MigrationError(
                    f"Target migration ID '{target}' has no known migration"
                )

            # moving forward
            if schema_version < target:
                if direction and direction != "up":
                    raise exceptions.DirectionError(
                        "Target would move version forward and direction is "
                        f"{direction}: can't go {schema_version} -> {target}",
                    )

                next_migration = find_next_migration(schema_version, self.migrations)

                while next_migration and next_migration.id <= target:
                    async with conn.transaction():
                        await self.run_sqlfile(next_migration.up, conn=conn)
                        await self._update_schema_version(
                            next_migration.id,
                            conn,
                        )
                    next_migration = next_migration.child

            # rolling back
            else:
                if direction and direction != "down":
                    raise exceptions.DirectionError(
                        "Target would roll back version and direction is "
                        f"{direction}: can't go {schema_version} -> {target}",
                    )
                try:
                    next_migration = self.migrations[schema_version]
                except KeyError:
                    raise exceptions.MigrationError(
                        f"Schema version '{schema_version}' "
                        "does not have associated migration",
                    )

                # build rollback chain
                chain: list[Migration] = []
                while next_migration and next_migration.id >= target:
                    if next_migration.down is None:
                        raise exceptions.MigrationError(
                            f"Cannot rollback from version {schema_version} "
                            f"to {target}: one or more migrations do not have "
                            "down files",
                        )

                    chain.append(next_migration)
                    next_migration = next_migration.parent

                for migration in chain:
                    # check here is simply to satisfy type checker;
                    # we shouldn't get here we a null down, but whatever
                    if migration.down is not None:
                        async with conn.transaction():
                            await self.run_sqlfile(migration.down, conn=conn)
                            await self._update_schema_version(
                                migration.id,
                                conn,
                            )

    async def verify(
        self,
        pg_dump: Optional[str] = None,
        output: Optional[TextIO] = None,
    ) -> bool:
        from dbami.pg_dump import pg_dump as _pg_dump

        if output is None:
            output = sys.stderr

        schema_db = random_name("dbami_verify_schema")
        migrate_db = random_name("dbami_verify_migrate")

        dump_args = [
            "--exclude-table",
            self.schema_version_table,
        ]

        async def schema():
            await self.create_database(schema_db)
            await self.load_schema(database=schema_db)
            return await _pg_dump("-d", schema_db, *dump_args, pg_dump=pg_dump)

        async def migrate():
            await self.create_database(migrate_db)
            await self.migrate(database=migrate_db)
            return await _pg_dump("-d", migrate_db, *dump_args, pg_dump=pg_dump)

        try:
            results: tuple[
                tuple[Optional[int], str],
                tuple[Optional[int], str],
            ] = await asyncio.gather(schema(), migrate())
        finally:
            await self.drop_database(schema_db)
            await self.drop_database(migrate_db)

        schema_results, migrate_results = results

        schema_rc, schema_dump = schema_results
        migrate_rc, migrate_dump = migrate_results

        if schema_rc != 0 or migrate_rc != 0:
            raise RuntimeError("Encoutered an error dumping databases")

        is_diff: bool = False

        if schema_dump != migrate_dump:
            import difflib

            is_diff = True
            output.writelines(
                difflib.unified_diff(
                    schema_dump.splitlines(keepends=True),
                    migrate_dump.splitlines(keepends=True),
                    fromfile="schema.sql",
                    tofile="combined migrations",
                )
            )

        return not is_diff

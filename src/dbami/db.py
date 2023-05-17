import asyncio
import asyncpg
from buildpg import render, V
from typing import Optional, AsyncGenerator
from pathlib import Path
from contextlib import asynccontextmanager, AsyncExitStack
import logging


logger = logging.getLogger(__name__)


SCHEMA_VERSION_TABLE = "schema_version"


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
        print(down_path, down_path.is_file())
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

    for migration in sorted(directory.glob("*.up.sql")):
        parent = Migration.from_up_path(migration, parent=parent)
        migrations[parent.id] = parent

    return migrations


def find_next_migration(
    current: int, migrations: dict[int, Migration]
) -> Optional[Migration]:
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
    raise ValueError(f"No migration path from schema version {current}")


class DB:
    def __init__(self, project: Path):
        self.project = project
        self.validate_project()
        self.schema = SqlFile(self.schema_file)
        self.migrations = migrations_from_dir(self.migrations_dir)
        self.tests = {
            f.name: f for f in (SqlFile(f) for f in self.tests_dir.glob("*.sql"))
        }
        self.fixtures = {
            f.name: f for f in (SqlFile(f) for f in self.fixtures_dir.glob("*.sql"))
        }

    @classmethod
    def project_schema(cls, project: Path):
        return project.joinpath("schema.sql")

    @classmethod
    def project_migrations(cls, project: Path):
        return project.joinpath("migrations")

    @classmethod
    def project_tests(cls, project: Path):
        return project.joinpath("tests")

    @classmethod
    def project_fixtures(cls, project: Path):
        return project.joinpath("fixtures")

    @classmethod
    def new_project(cls, directory: Path):
        cls.project_schema(directory).touch()
        cls.project_migrations(directory).mkdir(exist_ok=True)
        cls.project_tests(directory).mkdir(exist_ok=True)
        cls.project_fixtures(directory).mkdir(exist_ok=True)
        return cls(directory)

    @property
    def schema_file(self):
        return self.project_schema(self.project)

    @property
    def migrations_dir(self):
        return self.project_migrations(self.project)

    @property
    def tests_dir(self):
        return self.project_tests(self.project)

    @property
    def fixtures_dir(self):
        return self.project_fixtures(self.project)

    def validate_project(self):
        schema = self.schema_file
        migrations = self.migrations_dir
        fixtures = self.fixtures_dir
        tests = self.tests_dir

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

        if tests.exists() and not tests.is_dir():
            raise ValueError(
                f"Fixtures directory is not a directory: {migrations}",
            )

    def next_migration_id(self):
        return max(self.migrations.keys()) + 1 if self.migrations else 0

    def new_migration(
        self,
        name: str,
        up_content: Optional[str] = None,
        down_content: Optional[str] = None,
    ):
        _id = self.next_migration_id()
        base = self.migrations_dir.joinpath(f"{str(_id).zfill(5)}_{name}")
        up = base.with_suffix(".up.sql")
        down = base.with_suffix(".down.sql")
        up.touch()
        down.touch()

        if up_content:
            up.write_text(up_content)

        if down_content:
            down.write_text(down_content)

        self.migrations[_id] = Migration(
            _id,
            name,
            SqlFile(up),
            SqlFile(down),
            parent=self.migrations.get(_id - 1),
        )

    def new_fixture(self, name: str, content: Optional[str] = None):
        f = self.fixtures_dir.joinpath(f"{name}.sql")

        if f.exists():
            raise FileExistsError("Cannot create fixture, already exists: {f}")

        f.touch()

        if content:
            f.write_text(content)

        self.fixtures[name] = SqlFile(f)

    def new_test(self, name: str, content: Optional[str] = None):
        f = self.tests_dir.joinpath(f"{name}.sql")

        if f.exists():
            raise FileExistsError("Cannot create test, already exists: {f}")

        f.touch()

        if content:
            f.write_text(content)

        self.tests[name] = SqlFile(f)

    @staticmethod
    @asynccontextmanager
    async def get_db_connection(**kwargs):
        conn = None
        try:
            conn = await asyncpg.connect(**kwargs)
            yield conn
        finally:
            if conn:
                await conn.close()

    @classmethod
    async def create_database(cls, db_name: str, **kwargs) -> None:
        async with AsyncExitStack() as stack:
            kwargs["database"] = ""
            conn: asyncpg.Connection = kwargs.get(
                "conn"
            ) or await stack.enter_async_context(cls.get_db_connection(**kwargs))
            await conn.execute(f'CREATE DATABASE "{db_name}";')

    @classmethod
    async def drop_database(cls, db_name: str, **kwargs) -> None:
        async with AsyncExitStack() as stack:
            kwargs["database"] = ""
            conn: asyncpg.Connection = kwargs.get(
                "conn"
            ) or await stack.enter_async_context(cls.get_db_connection(**kwargs))
            await conn.execute(f'DROP DATABASE "{db_name}";')

    @classmethod
    async def run_sqlfile(cls, sqlfile: SqlFile, **kwargs) -> None:
        sql = sqlfile.path.read_text()

        if not sql:
            return

        async with AsyncExitStack() as stack:
            conn: asyncpg.Connection = kwargs.get(
                "conn"
            ) or await stack.enter_async_context(cls.get_db_connection(**kwargs))
            await conn.execute(sql)

    @classmethod
    async def get_current_version(
        cls,
        schema_version_table: str = SCHEMA_VERSION_TABLE,
        **kwargs,
    ) -> Optional[int]:
        async with AsyncExitStack() as stack:
            conn: asyncpg.Connection = kwargs.get(
                "conn"
            ) or await stack.enter_async_context(cls.get_db_connection(**kwargs))

            try:
                version = await conn.fetchval(
                    render(
                        """SELECT
    version
FROM :version_table
WHERE applied_at = (SELECT max(applied_at) from :version_table)
""",
                        version_table=V(schema_version_table),
                    )[0],
                )
            except asyncpg.UndefinedTableError:
                return None

        return version

    async def yield_unapplied_migrations(
        self, **kwargs
    ) -> AsyncGenerator[Migration, None]:
        schema_version = await self.get_current_version(**kwargs) or -1
        next_migration = find_next_migration(schema_version, self.migrations)

        while next_migration:
            yield next_migration
            next_migration = next_migration.child

    async def dump(self, *args) -> tuple[Optional[int], str]:
        proc = await asyncio.create_subprocess_exec(
            "pg_dump",
            *args,
            stdout=asyncio.subprocess.PIPE,
            # this just goes to the parent's stderr
            stderr=None,
            # no need for stdin, don't let it consume ours
            stdin=asyncio.subprocess.DEVNULL,
        )

        stdout, _ = await proc.communicate()

        return (proc.returncode, stdout.decode())

    async def load_schema(self, **kwargs):
        await self.run_sqlfile(self.schema, **kwargs)

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
        schema_version_table: str = SCHEMA_VERSION_TABLE,
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
            version_table=V(schema_version_table),
        )
        version_sql, params = render(
            "INSERT INTO :version_table (version) VALUES (:version)",
            version_table=V(schema_version_table),
            version=version,
        )

        try:
            # use a subtransaction (save point) to prevent this
            # "expected" exception from rolling back the migration
            async with conn.transaction():
                await conn.execute(table_sql)
        except asyncpg.InvalidSchemaNameError:
            schema = schema_version_table.split(".")[0]
            schema_sql, _ = render(
                "CREATE SCHEMA IF NOT EXISTS :schema",
                schema=V(schema),
            )
            await conn.execute(schema_sql)
            await conn.execute(table_sql)

        await conn.execute(version_sql, *params)

    async def migrate(
        self,
        target: Optional[int] = None,
        schema_version_table: str = SCHEMA_VERSION_TABLE,
        **kwargs,
    ):
        async with self.get_db_connection(**kwargs) as conn:
            schema_version: int = await self.get_current_version(conn=conn) or -1

            if target is None:
                target = max(self.migrations.keys())

                # if target was not specified then we never want to roll back
                if schema_version > target:
                    logger.warning(
                        "Current schema version % greater than all migrations",
                        schema_version,
                    )
                    return

            if schema_version == target:
                return

            if target not in self.migrations:
                raise ValueError(
                    f"Target migration ID '{target}' has no known migration"
                )

            # moving forward
            if schema_version < target:
                next_migration = find_next_migration(schema_version, self.migrations)

                while next_migration and next_migration.id <= target:
                    async with conn.transaction():
                        await self.run_sqlfile(next_migration.up, conn=conn)
                        await self._update_schema_version(
                            next_migration.id,
                            conn,
                            schema_version_table=schema_version_table,
                        )
                    next_migration = next_migration.child

            # rolling back
            else:
                try:
                    next_migration = self.migrations[schema_version]
                except KeyError:
                    raise ValueError(
                        f"Schema version '{schema_version}' "
                        "does not have associated migration",
                    )

                # build rollback chain
                chain: list[Migration] = []
                while next_migration and next_migration.id >= target:
                    if next_migration.down is None:
                        raise ValueError(
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
                                schema_version_table=schema_version_table,
                            )

    async def test(self):
        pass

    async def verify(
        self,
        schema_version_table: str = SCHEMA_VERSION_TABLE,
        **kwargs,
    ):
        # dump with --exclude-table-data SCHEMA_VERSION_TABLE
        # and get version separately
        pass

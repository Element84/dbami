# `dbami`: The database friend you didn't know you needed

A lightweight, Python-based, framework-agnostic, async-compatible PostgreSQL
migration tool.

## Features

- **Lightweight**: Minimal dependencies, focused solely on PostgreSQL
  migrations
- **Language-agnostic**: Works with any language or framework, no ORM required
- **Environment-aware**: Supports configuration via environment variables
- **Migration locking**: Prevents concurrent migrations for data safety
- **Schema verification**: Ensures your schema.sql and migrations stay in sync
- **Fixture support**: Load test data and fixtures easily
- **Async-compatible Python API**: Built on `asyncpg` for integration with
  modern async Python applications

## Installation

```bash
pip install dbami
```

## Quick Start

1. Initialize a new dbami project:

   ```bash
   dbami init
   ```

2. Create your first migration:

   ```bash
   dbami new create_users_table
   ```

3. Edit the generated migration files in the `migrations/` directory

4. Create and migrate your database:

   ```bash
   dbami up --database myapp_db
   ```

## Configuration

`dbami` can be configured using environment variables. All environment
variables are prefixed with `DBAMI_` (except for standard PostgreSQL connection
variables).

### Database Connection

`dbami` uses standard PostgreSQL environment variables for database
connections:

- `PGHOST` - Database host (default: localhost)
- `PGPORT` - Database port (default: 5432)
- `PGUSER` - Database user
- `PGPASSWORD` - Database password
- `PGDATABASE` - Database name

### dbami-specific Configuration

- `DBAMI_PROJECT_DIRECTORY` - Project directory containing migrations (default:
  current directory)
- `DBAMI_WAIT_TIMEOUT` - Seconds to wait for database connection (default: 60)
- `DBAMI_SCHEMA_VERSION_TABLE` - Name of the schema version tracking table
  (default: schema_version)

## CLI Reference

### Main Commands

<!-- [[[cog
import subprocess
result = subprocess.run(['dbami', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami [-h] [command] ...

The database friend you didn't know you needed.

options:
  -h, --help        show this help message and exit

commands:
  [command]
    init            Initialize a new dbami project (in the current directory unless specified)
    new             Create a new migration with the given name
    create          Create a database
    drop            Drop a database
    pending         List all unapplied migrations
    current-schema  Get current schema version
    load-schema     Load the schema.sql into a database
    migrate         Migrate the database to the latest (or specified) version
    rollback        Rollback the database to the last (or specified) version
    up              Migrate to the latest version, creating the database if necessary
    verify          Check that the schema and migrations are in sync
    version         Print the cli version
    list-fixtures   List all available fixture files on search path
    load-fixture    Load a sql fixture into the database
    execute-sql     Run SQL from stdin against the database
```
<!-- [[[end]]] -->

### Command Details

#### `init` - Initialize a new dbami project

<!-- [[[cog
result = subprocess.run(['dbami', 'init', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami init [-h] [--project-directory PROJECT_DIRECTORY]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
```
<!-- [[[end]]] -->

Creates the following structure:
```
.
├── migrations/
├── fixtures/
└── schema.sql
```

#### `new` - Create a new migration

<!-- [[[cog
result = subprocess.run(['dbami', 'new', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami new [-h] [--project-directory PROJECT_DIRECTORY] migration_name

positional arguments:
  migration_name

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
```
<!-- [[[end]]] -->

Creates two files:
- `migrations/YYYYMMDDHHMMSS_<name>.up.sql` - Apply migration
- `migrations/YYYYMMDDHHMMSS_<name>.down.sql` - Rollback migration

#### `create` - Create a database

<!-- [[[cog
result = subprocess.run(['dbami', 'create', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami create [-h] [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]

options:
  -h, --help            show this help message and exit
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
```
<!-- [[[end]]] -->

#### `drop` - Drop a database

<!-- [[[cog
result = subprocess.run(['dbami', 'drop', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami drop [-h] [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]

options:
  -h, --help            show this help message and exit
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
```
<!-- [[[end]]] -->

#### `migrate` - Apply migrations

<!-- [[[cog
result = subprocess.run(['dbami', 'migrate', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami migrate [-h] [--project-directory PROJECT_DIRECTORY]
                     [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]
                     [--target TARGET_MIGRATION_ID]
                     [--schema-version-table SCHEMA_VERSION_TABLE] [--no-lock]
                     [--lock-timeout LOCK_TIMEOUT]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
  --target TARGET_MIGRATION_ID
                        (default: 'latest')
  --schema-version-table SCHEMA_VERSION_TABLE
                        name of the table (optionally schema-qualified) in
                        which to store applied schema versions (default:
                        'schema_version'; env: $DBAMI_SCHEMA_VERSION_TABLE)
  --no-lock             do not lock db access during migration
  --lock-timeout LOCK_TIMEOUT
                        seconds to wait for db lock; 0 waits indefinitely
```
<!-- [[[end]]] -->

#### `rollback` - Rollback migrations

<!-- [[[cog
result = subprocess.run(['dbami', 'rollback', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami rollback [-h] [--project-directory PROJECT_DIRECTORY]
                      [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]
                      [--target TARGET_MIGRATION_ID]
                      [--schema-version-table SCHEMA_VERSION_TABLE]
                      [--no-lock] [--lock-timeout LOCK_TIMEOUT]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
  --target TARGET_MIGRATION_ID
                        (default: 'last')
  --schema-version-table SCHEMA_VERSION_TABLE
                        name of the table (optionally schema-qualified) in
                        which to store applied schema versions (default:
                        'schema_version'; env: $DBAMI_SCHEMA_VERSION_TABLE)
  --no-lock             do not lock db access during migration
  --lock-timeout LOCK_TIMEOUT
                        seconds to wait for db lock; 0 waits indefinitely
```
<!-- [[[end]]] -->

#### `up` - Create and migrate database

<!-- [[[cog
result = subprocess.run(['dbami', 'up', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami up [-h] [--project-directory PROJECT_DIRECTORY]
                [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]
                [--schema-version-table SCHEMA_VERSION_TABLE] [--no-lock]
                [--lock-timeout LOCK_TIMEOUT]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
  --schema-version-table SCHEMA_VERSION_TABLE
                        name of the table (optionally schema-qualified) in
                        which to store applied schema versions (default:
                        'schema_version'; env: $DBAMI_SCHEMA_VERSION_TABLE)
  --no-lock             do not lock db access during migration
  --lock-timeout LOCK_TIMEOUT
                        seconds to wait for db lock; 0 waits indefinitely
```
<!-- [[[end]]] -->

The `up` command is equivalent to running:
1. `create` (if database doesn't exist)
2. `migrate`

#### `pending` - List unapplied migrations

<!-- [[[cog
result = subprocess.run(['dbami', 'pending', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami pending [-h] [--project-directory PROJECT_DIRECTORY]
                     [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]
                     [--schema-version-table SCHEMA_VERSION_TABLE]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
  --schema-version-table SCHEMA_VERSION_TABLE
                        name of the table (optionally schema-qualified) in
                        which to store applied schema versions (default:
                        'schema_version'; env: $DBAMI_SCHEMA_VERSION_TABLE)
```
<!-- [[[end]]] -->

#### `current-schema` - Get current schema version

<!-- [[[cog
result = subprocess.run(['dbami', 'current-schema', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami current-schema [-h] [--project-directory PROJECT_DIRECTORY]
                            [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]
                            [--schema-version-table SCHEMA_VERSION_TABLE]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
  --schema-version-table SCHEMA_VERSION_TABLE
                        name of the table (optionally schema-qualified) in
                        which to store applied schema versions (default:
                        'schema_version'; env: $DBAMI_SCHEMA_VERSION_TABLE)
```
<!-- [[[end]]] -->

#### `load-schema` - Load schema.sql

<!-- [[[cog
result = subprocess.run(['dbami', 'load-schema', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami load-schema [-h] [--project-directory PROJECT_DIRECTORY]
                         [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
```
<!-- [[[end]]] -->

#### `verify` - Verify schema consistency

<!-- [[[cog
result = subprocess.run(['dbami', 'verify', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami verify [-h] [--project-directory PROJECT_DIRECTORY]
                    [--wait-timeout WAIT_TIMEOUT]
                    [--schema-version-table SCHEMA_VERSION_TABLE]
                    [--pg-dump PG_DUMP]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  --schema-version-table SCHEMA_VERSION_TABLE
                        name of the table (optionally schema-qualified) in
                        which to store applied schema versions (default:
                        'schema_version'; env: $DBAMI_SCHEMA_VERSION_TABLE)
  --pg-dump PG_DUMP     path to pg_dump executable or name to lookup on path
                        (default: 'pg_dump'; env: $DBAMI_PG_DUMP)
```
<!-- [[[end]]] -->

#### `list-fixtures` - List available fixtures

<!-- [[[cog
result = subprocess.run(['dbami', 'list-fixtures', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami list-fixtures [-h] [--project-directory PROJECT_DIRECTORY]
                           [--fixture-dir FIXTURE_DIRS]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --fixture-dir FIXTURE_DIRS
                        directory from which to load sql fixtures; later
                        directories take precedence
```
<!-- [[[end]]] -->

#### `load-fixture` - Load a fixture

<!-- [[[cog
result = subprocess.run(['dbami', 'load-fixture', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami load-fixture [-h] [--project-directory PROJECT_DIRECTORY]
                          [--fixture-dir FIXTURE_DIRS]
                          [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]
                          fixture_name

positional arguments:
  fixture_name          name of fixture to load

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --fixture-dir FIXTURE_DIRS
                        directory from which to load sql fixtures; later
                        directories take precedence
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
```
<!-- [[[end]]] -->

#### `execute-sql` - Execute SQL from stdin

<!-- [[[cog
result = subprocess.run(['dbami', 'execute-sql', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami execute-sql [-h] [--project-directory PROJECT_DIRECTORY]
                         [--wait-timeout WAIT_TIMEOUT] [-d DATABASE_NAME]

options:
  -h, --help            show this help message and exit
  --project-directory PROJECT_DIRECTORY
                        (default: '/Users/jkeifer/e84/dbami/dbami'; env:
                        $DBAMI_PROJECT_DIRECTORY)
  --wait-timeout WAIT_TIMEOUT
                        seconds to wait for db connection (default: '60'; env:
                        $DBAMI_WAIT_TIMEOUT)
  -d DATABASE_NAME, --database DATABASE_NAME
                        (required; env: $PGDATABASE)
```
<!-- [[[end]]] -->

Example:

```bash
echo "SELECT version();" | dbami execute-sql -d mydb
```

#### `version` - Show version

<!-- [[[cog
result = subprocess.run(['dbami', 'version', '--help'], capture_output=True, text=True)
cog.out('```\n')
cog.out(result.stdout)
cog.out('```\n')
]]] -->
```
usage: dbami version [-h]

options:
  -h, --help  show this help message and exit
```
<!-- [[[end]]] -->

## Project Structure

A dbami project structure might look like this:

```
myproject/
├── migrations/
│   ├── 20240115120000_create_users_table.up.sql
│   ├── 20240115120000_create_users_table.down.sql
│   ├── 20240116093000_add_email_to_users.up.sql
│   └── 20240116093000_add_email_to_users.down.sql
├── fixtures/
│   ├── test_users.sql
│   └── sample_data.sql
└── schema.sql
```

The cli `init` command can start this for you. `new` will create new
migration files.

### Migrations

Migrations are SQL files that define database changes:

- **Up migrations** (`*.up.sql`): Apply changes
- **Down migrations** (`*.down.sql`): Rollback changes

Migration files are named with the pattern:
`YYYYMMDDHHMMSS_description.{up|down}.sql`

### Schema

The `schema.sql` file contains the complete database schema. This file should
be maintained to reflect the current state of your database schema after all
migrations have been applied. The `verify` command can help ensure your
schema.sql and migrations stay in sync.

To understand the motivations behind this design, see [this discussion in the
dbmate repo](https://github.com/amacneil/dbmate/discussions/433).

### Fixtures

Fixtures are SQL files containing test data. They can be loaded using:

```bash
dbami load-fixture test_users -d mydb
```

## Advanced Usage

### Migration Locking

By default, dbami uses advisory locks to prevent concurrent migrations. You can
disable this with `--no-lock`:

```bash
dbami migrate -d mydb --no-lock
```

### Custom Schema Version Table

You can use a custom table name or schema for tracking migrations:

```bash
dbami migrate -d mydb --schema-version-table myschema.migrations
```

### Migration Targets

Migrate to a specific version:

```bash
# Migrate up to (and including) a specific migration
dbami migrate -d mydb --target 20240115120000

# Rollback to a specific migration
dbami rollback -d mydb --target 20240115120000
```

### Programmatic Usage

While dbami includes a comprehensive CLI, you can also use it programmatically
via its async Python API:

```python
from dbami.db import DB
from dbami.util import syncrun

async def setup_database():
    db = DB("/path/to/project")
    await db.create_database("mydb")
    await db.migrate("mydb")

# Run async function
syncrun(setup_database())
```

## Development

### Setting up for development

```bash
# Clone the repository
git clone https://github.com/yourusername/dbami.git
cd dbami

# Install with development dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

### Regenerating this README

This README uses [cogapp](https://github.com/nedbat/cog) to keep the CLI
documentation up-to-date. To regenerate:

```bash
cog -r README.md
```

## What does `dbami` mean?

`dbami` was initially inspired by the tool
[`dbmate`](https://github.com/amacneil/dbmate), and therefore takes inspiration
from `dbmate` when it comes to name. That is, "ami" is French for "friend," a
synonym of "mate."

Other languages may have yielded suitable words for friend, but "ami" was
chosen due to its short length (easier to type) and the allowance for a future
golang-based implementation, which, most serendipitously, could be named
`dbamigo`.

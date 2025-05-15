# pg-backup-restore

`pg-backup-restore` is a Go command-line tool for backing up and restoring PostgreSQL databases. It supports dumping table schemas and data to SQL files and restoring them to another database, with specific compatibility for Neon-hosted databases requiring SSL connections. The tool is designed for use with Django databases (e.g., tables like `public.django_migrations`) but works with any PostgreSQL database.

## Features

- **Backup**: Dumps database schemas and data to timestamped SQL files (e.g., `neondb_20250515_134805.sql`).
- **Restore**: Restores data from the latest backup file to a specified database.
- **Interactive Prompts**: User-friendly prompts for database connection details, including SSL mode for Neon compatibility.
- **Neon Support**: Configurable SSL mode (`require`, `verify-full`, `disable`) for secure connections to Neon databases.
- **Custom DDL Generation**: Generates `CREATE TABLE` statements using PostgreSQL system catalogs, avoiding reliance on non-standard functions like `pg_get_tabledef`.
- **Type Handling**: Supports PostgreSQL data types, including `jsonb`, `text[]`, and nested arrays, using reflection for compatibility with `pgx/v5.7.4`.

## Prerequisites

- **Go**: Version 1.18 or later.
- **PostgreSQL**: Version 12 or later (tested with Neon-hosted databases).
- **Dependencies**:
  - `github.com/jackc/pgx/v5 v5.7.4`
  - `github.com/manifoldco/promptui`
- **Neon Database**: A Neon account with connection details (host, port, user, password, database name).

## Installation

1. **Clone the Repository** (or create a new directory for the project):

   ```bash
   git clone <repository-url> pg-backup-restore
   cd pg-backup-restore
   ```

   Or create a new directory and place `main.go` there.

2. **Install Dependencies**:

   ```bash
   go mod init pg-backup-restore
   go get github.com/jackc/pgx/v5@v5.7.4
   go get github.com/manifoldco/promptui
   ```

3. **Build the Program**:

   ```bash
   go build -o pg-backup-restore
   ```

## Usage

Run the program and follow the interactive prompts to back up or restore a PostgreSQL database.

```bash
./pg-backup-restore
```

### Backup

1. Select the `dump` action.
2. Enter connection details:
   - **Database name**: e.g., `neondb`
   - **Host**: e.g., `ep-still-frog-b9q1o36e-pooler.us-east-1.aws.neon.tech`
   - **Port**: e.g., `5432`
   - **User**: e.g., `neondb_owner`
   - **Password**: Your database password
   - **Schema**: e.g., `public` (or leave empty for all schemas)
   - **Backup directory**: e.g., `./backups`
   - **SSL mode**: `require` (recommended for Neon)
3. The program generates a timestamped SQL file in the backup directory (e.g., `./backups/neondb_20250515_134805.sql`).

### Restore

1. Select the `restore` action.
2. Enter connection details for the target database (same fields as above).
3. The program finds the latest backup file for the specified database and restores it.

### Example Output

**Backup File** (`./backups/neondb_20250515_134805.sql`):

```sql
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE public.django_migrations (
    id INTEGER NOT NULL PRIMARY KEY,
    app VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    applied TIMESTAMP WITH TIME ZONE NOT NULL
);

ALTER TABLE public.django_migrations DISABLE TRIGGER ALL;
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (...);
ALTER TABLE public.django_migrations ENABLE TRIGGER ALL;
```

## Testing

### Setup

1. **Create a Test Database** (on Neon or local PostgreSQL):

   ```sql
   CREATE DATABASE testdb;
   \c testdb
   CREATE SCHEMA public;
   CREATE TABLE public.example (
       id SERIAL PRIMARY KEY,
       data JSONB,
       tags TEXT[],
       numbers INT[],
       nested_tags TEXT[][]
   );
   INSERT INTO public.example (data, tags, numbers, nested_tags) VALUES
       ('{"key": "value"}', ARRAY['tag1', 'tag2'], ARRAY[1, 2, 3], ARRAY[ARRAY['a', 'b'], ARRAY['c']]),
       ('{"num": 42}', ARRAY['tag3'], ARRAY[4], ARRAY[ARRAY['d']]),
       (NULL, NULL, NULL, NULL);
   ```

2. **Run Backup**:

   ```bash
   ./pg-backup-restore
   ```

   - Select `dump`.
   - Enter Neon connection details (e.g., `neondb`, `ep-still-frog-a4q1o3ae-pooler.us-east-1.aws.neon.tech`, `5432`, `neondb_owner`, password, `public`, `./backups`, `require`).
   - Verify the SQL file in `./backups`.

3. **Run Restore**:

   - Create a new Neon database: `CREATE DATABASE testdb_restore;`
   - Select `restore`.
   - Enter connection details for `testdb_restore`.
   - Verify the data:

   ```sql
   \c testdb_restore
   SELECT * FROM public.example;
   ```

   Expected output:

   ```
    id |       data        |    tags    | numbers |      nested_tags
   ----+-------------------+------------+---------+----------------------
     1 | {"key": "value"}  | {tag1,tag2}| {1,2,3} | {{a,b},{c}}
     2 | {"num": 42}       | {tag3}     | {4}     | {{d}}
     3 | NULL              | NULL       | NULL    | NULL
   ```

### Logs

Check logs for debugging:

```
2025/05/15 13:55:00 Connecting to database neondb at ep-still-frog-a4q1o3ae-pooler.us-east-1.aws.neon.tech:5432
2025/05/15 13:55:00 Dumping tables for schema public
2025/05/15 13:55:00 Generating DDL for table public.django_migrations
```

## Troubleshooting

- **Connection Error** (`ERROR: connection is insecure`):
  - Ensure `sslmode=require` is selected during the prompt.
  - Verify Neon connection details in the Neon dashboard.
  - Test manually with `psql`:

    ```bash
    psql "host=ep-still-frog-a4q1o3ae-pooler.us-east-1.aws.neon.tech port=5432 user=neondb_owner password=your_password dbname=neondb sslmode=require"
    ```

- **DDL Generation Error**:
  - Check PostgreSQL version: `SELECT version();`
  - Ensure system catalogs (`pg_class`, `pg_constraint`) are accessible.
  - Run the DDL query manually in `psql`:

    ```sql
    SELECT 
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.is_nullable,
        c.column_default,
        CASE 
            WHEN pc.contype = 'p' THEN pg_catalog.pg_get_constraintdef(pc.oid, true)
            ELSE NULL 
        END AS primary_key
    FROM information_schema.columns c
    JOIN pg_catalog.pg_class cl ON cl.relname = c.table_name
    JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = c.table_schema
    LEFT JOIN pg_catalog.pg_constraint pc ON pc.conrelid = cl.oid 
        AND pc.contype = 'p' 
        AND pc.conkey @> ARRAY[c.ordinal_position::smallint]
    WHERE c.table_schema = 'public' AND c.table_name = 'django_migrations'
    ORDER BY c.ordinal_position;
    ```

- **Conn Busy Error**:
  - Ensure queries are executed sequentially (fixed in `dumpSchemaTables`).
  - Check for concurrent connections: `SELECT * FROM pg_stat_activity WHERE datname = 'neondb';`

- **Build Issues**:
  - Clear module cache: `go clean -modcache && go mod download`
  - Verify dependencies: `go mod why github.com/jackc/pgx/v5`

## Limitations

- **Complex Constraints**: The DDL generator supports primary keys, foreign keys, unique constraints, and indexes but may not handle all edge cases (e.g., complex check constraints).
- **Performance**: Single-row `INSERT` statements may be slow for large tables; consider batch inserts (see improvements below).
- **Neon-Specific**: Optimized for Neon with `sslmode=require`; other cloud providers may require different SSL settings.

## Future Improvements

- **Batch Inserts**: Implement batch `INSERT` statements for better performance.
- **Connection Pool**: Use `pgxpool.Pool` for Neon’s connection pooler.
- **Environment Variables**: Support `PGPASSWORD` and other environment variables for sensitive data.
- **pg_dump Fallback**: Use `pg_dump` for complex DDL generation.
- **Transaction Control**: Add read-only transactions for consistent backups.

## License

MIT License. See `LICENSE` for details.

## Contact

For issues or contributions, open a GitHub issue or contact the maintainer.
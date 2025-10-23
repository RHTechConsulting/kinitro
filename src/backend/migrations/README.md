# Kinitro Backend Database Migrations

This directory contains Alembic migrations for the Kinitro Backend database.

## Setup

1. **Configure Database URL**:
   Set the environment variable `DATABASE_URL` or update the URL in `alembic.ini`:

   ```bash
   export DATABASE_URL="postgresql://user:password@localhost/kinitrodb"
   ```

2. **Install Dependencies**:

   ```bash
   uv sync
   uv sync --dev
   ```

## Running Migrations

### Apply Migrations

```bash
# From the backend directory
cd src/backend
alembic upgrade head
```

### Generate New Migration

```bash
# Auto-generate migration from model changes
alembic revision --autogenerate -m "description of changes"

# Create empty migration file
alembic revision -m "description of changes"
```

### Migration History

```bash
# Show current revision
alembic current

# Show migration history
alembic history --verbose

# Show pending migrations
alembic show head
```

### Downgrade

```bash
# Downgrade to previous migration
alembic downgrade -1

# Downgrade to specific revision
alembic downgrade <revision_id>

# Downgrade all (drop all tables)
alembic downgrade base
```

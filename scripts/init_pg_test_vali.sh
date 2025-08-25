dropdb postgres
dropdb postgres_child
createdb postgres
createdb postgres_child
uv run alembic upgrade head
DATABASE_URL=postgresql://postgres@localhost/postgres uv run alembic upgrade head
DATABASE_URL=postgresql://postgres@localhost/postgres_child uv run alembic upgrade head
PGDATABASE=postgres uv run pgq install
PGDATABASE=postgres_child uv run pgq install

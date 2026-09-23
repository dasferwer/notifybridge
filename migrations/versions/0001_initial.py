"""Создаёт получателей, доставки, журнал попыток и таблицы локального эмулятора."""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

DDL = """
CREATE TABLE recipients (id UUID PRIMARY KEY, config JSON NOT NULL, version INTEGER NOT NULL);
CREATE TABLE events (id UUID PRIMARY KEY, recipient_id UUID NOT NULL REFERENCES recipients(id),
 fingerprint VARCHAR(64) NOT NULL, delivery_ids JSON NOT NULL, suppressed JSON NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE deliveries (id UUID PRIMARY KEY, recipient_id UUID NOT NULL REFERENCES recipients(id),
 channel VARCHAR(20) NOT NULL, destination VARCHAR(200) NOT NULL, group_key VARCHAR(200),
 priority INTEGER NOT NULL, items JSON NOT NULL, preferences JSON NOT NULL, envelope JSON,
 sealed BOOLEAN NOT NULL, status VARCHAR(20) NOT NULL, due_at TIMESTAMPTZ NOT NULL,
 attempts INTEGER NOT NULL, lease_token UUID, lease_until TIMESTAMPTZ, receipt_id VARCHAR(80),
 last_error VARCHAR(80), created_at TIMESTAMPTZ NOT NULL DEFAULT now(), sent_at TIMESTAMPTZ,
 CONSTRAINT ck_delivery_status CHECK (status IN ('pending','sending','sent','dead')),
 CONSTRAINT ck_delivery_attempts CHECK (attempts >= 0));
CREATE INDEX ix_deliveries_recipient_id ON deliveries (recipient_id);
CREATE INDEX ix_deliveries_group_key ON deliveries (group_key);
CREATE INDEX ix_deliveries_ready ON deliveries (status, due_at, priority);
CREATE TABLE outbox (id UUID PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), published_at TIMESTAMPTZ);
CREATE INDEX ix_outbox_published_at ON outbox (published_at);
CREATE TABLE attempts (id UUID PRIMARY KEY, delivery_id UUID NOT NULL REFERENCES deliveries(id),
 number INTEGER NOT NULL, outcome VARCHAR(80) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX ix_attempts_delivery_id ON attempts (delivery_id);
CREATE TABLE provider_receipts (key UUID PRIMARY KEY, fingerprint VARCHAR(64) NOT NULL, envelope JSON NOT NULL,
 receipt_id VARCHAR(80) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE provider_modes (channel VARCHAR(20) PRIMARY KEY, mode VARCHAR(30) NOT NULL);
"""


def upgrade():
    for statement in DDL.split(";"):
        if statement.strip():
            op.execute(statement)


def downgrade():
    for table in (
        "provider_modes",
        "provider_receipts",
        "attempts",
        "outbox",
        "deliveries",
        "events",
        "recipients",
    ):
        op.drop_table(table)

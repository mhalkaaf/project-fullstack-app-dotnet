CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE products (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL UNIQUE,
    description text NOT NULL DEFAULT '',
    price numeric(12, 2) NOT NULL CHECK (price >= 0),
    available_quantity integer NOT NULL CHECK (available_quantity >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_name text NOT NULL CHECK (length(trim(customer_name)) BETWEEN 1 AND 120),
    total_amount numeric(12, 2) NOT NULL CHECK (total_amount >= 0),
    status text NOT NULL CHECK (status IN (
        'PENDING',
        'INVENTORY_RESERVED',
        'INVENTORY_REJECTED',
        'PAYMENT_APPROVED',
        'PAYMENT_REJECTED',
        'COMPLETED',
        'FAILED'
    )),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id uuid NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id uuid NOT NULL REFERENCES products(id),
    quantity integer NOT NULL CHECK (quantity > 0),
    unit_price numeric(12, 2) NOT NULL CHECK (unit_price >= 0),
    UNIQUE (order_id, product_id)
);

CREATE TABLE order_events (
    id uuid PRIMARY KEY,
    order_id uuid NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    event_data jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX order_events_order_created_idx ON order_events (order_id, created_at);

CREATE TABLE outbox_events (
    id uuid PRIMARY KEY,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    topic text NOT NULL,
    payload jsonb NOT NULL,
    traceparent text,
    attempts integer NOT NULL DEFAULT 0,
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX outbox_unpublished_idx ON outbox_events (created_at)
    WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id uuid NOT NULL,
    consumer_name text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_name)
);

CREATE TABLE dead_letter_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id uuid,
    topic text NOT NULL,
    event_key text,
    payload jsonb NOT NULL,
    error_message text NOT NULL,
    attempts integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    retried_at timestamptz
);

INSERT INTO products (name, description, price, available_quantity) VALUES
    ('Mechanical Keyboard', 'Hot-swappable 75% keyboard', 89.00, 20),
    ('USB-C Dock', 'Eight-port developer workstation dock', 119.00, 15),
    ('Observability Mug', 'A mug for staring at dashboards', 14.50, 50),
    ('Laptop Stand', 'Adjustable aluminum stand', 42.00, 25)
ON CONFLICT (name) DO NOTHING;

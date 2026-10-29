import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { context, propagation, trace } from '@opentelemetry/api';
import { Kafka, logLevel as kafkaLogLevel } from 'kafkajs';
import pg from 'pg';
import pino from 'pino';
import client from 'prom-client';

const { Pool } = pg;
const port = Number(process.env.PORT ?? 3000);
const service = process.env.OTEL_SERVICE_NAME ?? 'orderflow-api';
const logger = pino({ level: process.env.LOG_LEVEL ?? 'info', base: { service } });
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const kafka = new Kafka({
  clientId: service,
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(','),
  logLevel: kafkaLogLevel.NOTHING
});
const producer = kafka.producer({ allowAutoTopicCreation: false });
let kafkaReady = false;
let shuttingDown = false;

client.collectDefaultMetrics({ prefix: 'orderflow_api_' });
const httpRequests = new client.Counter({
  name: 'orderflow_http_requests_total',
  help: 'HTTP requests handled by the OrderFlow API',
  labelNames: ['method', 'route', 'status_code']
});
const httpDuration = new client.Histogram({
  name: 'orderflow_http_request_duration_seconds',
  help: 'OrderFlow API request duration',
  labelNames: ['method', 'route'],
  buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5]
});
const ordersCreated = new client.Counter({
  name: 'orderflow_orders_created_total',
  help: 'Orders accepted by the API'
});
const kafkaMessages = new client.Counter({
  name: 'orderflow_kafka_messages_total',
  help: 'Kafka messages handled by direction and result',
  labelNames: ['service', 'direction', 'topic', 'result']
});
const outboxPending = new client.Gauge({
  name: 'orderflow_outbox_pending',
  help: 'Number of unpublished transactional outbox events'
});

function writeJson(response, statusCode, value) {
  response.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'access-control-allow-origin': '*',
    'access-control-allow-headers': 'content-type',
    'access-control-allow-methods': 'GET,POST,OPTIONS'
  });
  response.end(JSON.stringify(value));
}

async function readJson(request) {
  let body = '';
  for await (const chunk of request) {
    body += chunk;
    if (body.length > 1_000_000) throw new Error('Request body is too large');
  }
  try {
    return JSON.parse(body || '{}');
  } catch {
    const error = new Error('Request body must be valid JSON');
    error.statusCode = 400;
    throw error;
  }
}

function routeName(method, pathname) {
  if (pathname === '/api/products') return `${method} /api/products`;
  if (pathname === '/api/orders') return `${method} /api/orders`;
  if (/^\/api\/orders\/[0-9a-f-]+$/.test(pathname)) return `${method} /api/orders/:id`;
  if (pathname.startsWith('/health/')) return `${method} /health/:kind`;
  if (pathname === '/metrics') return `${method} /metrics`;
  return `${method} unmatched`;
}

function currentTrace() {
  const spanContext = trace.getSpan(context.active())?.spanContext();
  const carrier = {};
  propagation.inject(context.active(), carrier);
  return { traceId: spanContext?.traceId ?? null, traceparent: carrier.traceparent ?? null };
}

async function createOrder(payload) {
  const customerName = typeof payload.customerName === 'string' ? payload.customerName.trim() : '';
  const items = Array.isArray(payload.items) ? payload.items : [];
  if (!customerName || customerName.length > 120) {
    const error = new Error('customerName must contain between 1 and 120 characters');
    error.statusCode = 400;
    throw error;
  }
  if (items.length === 0 || items.some((item) => !item.productId || !Number.isInteger(item.quantity) || item.quantity < 1)) {
    const error = new Error('items must contain a productId and a positive integer quantity');
    error.statusCode = 400;
    throw error;
  }

  const normalized = new Map();
  for (const item of items) {
    normalized.set(item.productId, (normalized.get(item.productId) ?? 0) + item.quantity);
  }

  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const productIds = [...normalized.keys()];
    const result = await db.query(
      'SELECT id, name, price, available_quantity FROM products WHERE id = ANY($1::uuid[]) FOR SHARE',
      [productIds]
    );
    if (result.rows.length !== productIds.length) {
      const error = new Error('One or more products do not exist');
      error.statusCode = 400;
      throw error;
    }

    let total = 0;
    for (const product of result.rows) {
      total += Number(product.price) * normalized.get(product.id);
    }

    const orderId = randomUUID();
    const eventId = randomUUID();
    const occurredAt = new Date().toISOString();
    const telemetry = currentTrace();
    const event = {
      eventId,
      eventType: 'OrderCreated',
      eventVersion: 1,
      occurredAt,
      traceId: telemetry.traceId,
      data: {
        orderId,
        customerName,
        totalAmount: total.toFixed(2),
        items: result.rows.map((product) => ({
          productId: product.id,
          name: product.name,
          quantity: normalized.get(product.id),
          unitPrice: product.price
        }))
      }
    };

    await db.query(
      'INSERT INTO orders (id, customer_name, total_amount, status) VALUES ($1, $2, $3, $4)',
      [orderId, customerName, total.toFixed(2), 'PENDING']
    );
    for (const product of result.rows) {
      await db.query(
        'INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES ($1, $2, $3, $4)',
        [orderId, product.id, normalized.get(product.id), product.price]
      );
    }
    await db.query(
      'INSERT INTO order_events (id, order_id, event_type, event_data) VALUES ($1, $2, $3, $4)',
      [eventId, orderId, 'OrderCreated', JSON.stringify(event)]
    );
    await db.query(
      'INSERT INTO outbox_events (id, aggregate_id, event_type, topic, payload, traceparent) VALUES ($1, $2, $3, $4, $5, $6)',
      [eventId, orderId, 'OrderCreated', 'orders.created', JSON.stringify(event), telemetry.traceparent]
    );
    await db.query('COMMIT');
    ordersCreated.inc();
    logger.info({ orderId, eventId, traceId: telemetry.traceId }, 'order created');
    return { id: orderId, status: 'PENDING', totalAmount: total.toFixed(2) };
  } catch (error) {
    await db.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    db.release();
  }
}

async function getOrder(orderId) {
  const result = await pool.query(
    `SELECT o.id, o.customer_name, o.total_amount, o.status, o.created_at, o.updated_at,
      COALESCE(json_agg(DISTINCT jsonb_build_object(
        'productId', p.id, 'name', p.name, 'quantity', oi.quantity, 'unitPrice', oi.unit_price
      )) FILTER (WHERE p.id IS NOT NULL), '[]') AS items
     FROM orders o
     LEFT JOIN order_items oi ON oi.order_id = o.id
     LEFT JOIN products p ON p.id = oi.product_id
     WHERE o.id = $1
     GROUP BY o.id`,
    [orderId]
  );
  if (!result.rows[0]) return null;
  const events = await pool.query(
    'SELECT id, event_type, event_data, created_at FROM order_events WHERE order_id = $1 ORDER BY created_at, id',
    [orderId]
  );
  return { ...result.rows[0], events: events.rows };
}

async function publishOutbox() {
  if (!kafkaReady || shuttingDown) return;
  const result = await pool.query(
    'SELECT id, aggregate_id, topic, payload, traceparent FROM outbox_events WHERE published_at IS NULL ORDER BY created_at LIMIT 25'
  );
  for (const row of result.rows) {
    try {
      const headers = row.traceparent ? { traceparent: row.traceparent } : undefined;
      await producer.send({
        topic: row.topic,
        messages: [{ key: row.aggregate_id, value: JSON.stringify(row.payload), headers }]
      });
      await pool.query('UPDATE outbox_events SET published_at = now(), attempts = attempts + 1 WHERE id = $1', [row.id]);
      kafkaMessages.inc({ service, direction: 'produced', topic: row.topic, result: 'success' });
      logger.info({ orderId: row.aggregate_id, eventId: row.id, topic: row.topic }, 'outbox event published');
    } catch (error) {
      await pool.query('UPDATE outbox_events SET attempts = attempts + 1 WHERE id = $1', [row.id]).catch(() => undefined);
      kafkaMessages.inc({ service, direction: 'produced', topic: row.topic, result: 'error' });
      logger.error({ err: error, orderId: row.aggregate_id, eventId: row.id, topic: row.topic }, 'outbox publish failed');
      break;
    }
  }
  const pending = await pool.query('SELECT count(*)::int AS count FROM outbox_events WHERE published_at IS NULL');
  outboxPending.set(pending.rows[0].count);
}

const server = http.createServer(async (request, response) => {
  const started = process.hrtime.bigint();
  const url = new URL(request.url, `http://${request.headers.host ?? 'localhost'}`);
  const route = routeName(request.method, url.pathname);
  let statusCode = 500;

  try {
    if (request.method === 'OPTIONS') {
      statusCode = 204;
      response.writeHead(204, {
        'access-control-allow-origin': '*',
        'access-control-allow-headers': 'content-type',
        'access-control-allow-methods': 'GET,POST,OPTIONS'
      });
      response.end();
      return;
    }
    if (request.method === 'GET' && url.pathname === '/health/live') {
      statusCode = 200;
      return writeJson(response, 200, { status: 'ok', service });
    }
    if (request.method === 'GET' && url.pathname === '/health/ready') {
      await pool.query('SELECT 1');
      statusCode = kafkaReady ? 200 : 503;
      return writeJson(response, statusCode, { status: kafkaReady ? 'ready' : 'not_ready', postgres: true, kafka: kafkaReady });
    }
    if (request.method === 'GET' && url.pathname === '/metrics') {
      statusCode = 200;
      response.writeHead(200, { 'content-type': client.register.contentType });
      response.end(await client.register.metrics());
      return;
    }
    if (request.method === 'GET' && url.pathname === '/api/products') {
      const result = await pool.query('SELECT id, name, description, price, available_quantity FROM products ORDER BY name');
      statusCode = 200;
      return writeJson(response, 200, result.rows);
    }
    if (request.method === 'GET' && url.pathname === '/api/orders') {
      const result = await pool.query(
        'SELECT id, customer_name, total_amount, status, created_at, updated_at FROM orders ORDER BY created_at DESC LIMIT 50'
      );
      statusCode = 200;
      return writeJson(response, 200, result.rows);
    }
    if (request.method === 'GET' && /^\/api\/orders\/[0-9a-f-]+$/.test(url.pathname)) {
      const order = await getOrder(url.pathname.split('/').at(-1));
      statusCode = order ? 200 : 404;
      return writeJson(response, statusCode, order ?? { error: 'Order not found' });
    }
    if (request.method === 'POST' && url.pathname === '/api/orders') {
      const order = await createOrder(await readJson(request));
      statusCode = 202;
      return writeJson(response, 202, order);
    }
    statusCode = 404;
    return writeJson(response, 404, { error: 'Route not found' });
  } catch (error) {
    statusCode = error.statusCode ?? 500;
    logger.error({ err: error, method: request.method, path: url.pathname, traceId: currentTrace().traceId }, 'request failed');
    return writeJson(response, statusCode, { error: statusCode >= 500 ? 'Internal server error' : error.message });
  } finally {
    const seconds = Number(process.hrtime.bigint() - started) / 1e9;
    httpRequests.inc({ method: request.method, route, status_code: String(statusCode) });
    httpDuration.observe({ method: request.method, route }, seconds);
  }
});

async function start() {
  await producer.connect();
  kafkaReady = true;
  setInterval(() => publishOutbox().catch((error) => logger.error({ err: error }, 'outbox cycle failed')), 1000).unref();
  server.listen(port, '0.0.0.0', () => logger.info({ port }, 'api listening'));
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  kafkaReady = false;
  logger.info({ signal }, 'shutting down');
  server.close();
  await Promise.allSettled([producer.disconnect(), pool.end()]);
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start().catch((error) => {
  logger.fatal({ err: error }, 'api startup failed');
  process.exit(1);
});

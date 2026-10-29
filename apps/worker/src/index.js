import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { context, propagation, SpanStatusCode, trace } from '@opentelemetry/api';
import { Kafka, logLevel as kafkaLogLevel } from 'kafkajs';
import pg from 'pg';
import pino from 'pino';
import client from 'prom-client';

const { Pool } = pg;
const port = Number(process.env.PORT ?? 3001);
const service = process.env.OTEL_SERVICE_NAME ?? 'orderflow-worker';
const groupId = process.env.KAFKA_GROUP_ID ?? 'orderflow-worker';
const paymentFailureRate = Math.min(1, Math.max(0, Number(process.env.PAYMENT_FAILURE_RATE ?? 0.1)));
const logger = pino({ level: process.env.LOG_LEVEL ?? 'info', base: { service } });
const tracer = trace.getTracer(service);
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const kafka = new Kafka({
  clientId: service,
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(','),
  logLevel: kafkaLogLevel.NOTHING,
  retry: { retries: 5 }
});
const consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false });
const producer = kafka.producer({ allowAutoTopicCreation: false });
let ready = false;
let shuttingDown = false;

client.collectDefaultMetrics({ prefix: 'orderflow_worker_' });
const kafkaMessages = new client.Counter({
  name: 'orderflow_kafka_messages_total',
  help: 'Kafka messages handled by direction and result',
  labelNames: ['service', 'direction', 'topic', 'result']
});
const ordersProcessed = new client.Counter({
  name: 'orderflow_orders_processed_total',
  help: 'Orders reaching a terminal state',
  labelNames: ['outcome']
});
const processingDuration = new client.Histogram({
  name: 'orderflow_order_processing_duration_seconds',
  help: 'Time from order creation to terminal state',
  labelNames: ['outcome'],
  buckets: [0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60]
});
const deadLetters = new client.Counter({
  name: 'orderflow_dead_letter_total',
  help: 'Events written to the dead-letter topic',
  labelNames: ['source_topic']
});

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function traceFields() {
  const spanContext = trace.getSpan(context.active())?.spanContext();
  const carrier = {};
  propagation.inject(context.active(), carrier);
  return { traceId: spanContext?.traceId ?? null, traceparent: carrier.traceparent ?? null };
}

async function addTransition(db, sourceEvent, eventType, topic, status, extra = {}) {
  const eventId = randomUUID();
  const telemetry = traceFields();
  const payload = {
    eventId,
    eventType,
    eventVersion: 1,
    occurredAt: new Date().toISOString(),
    traceId: telemetry.traceId ?? sourceEvent.traceId ?? null,
    causationId: sourceEvent.eventId,
    data: { orderId: sourceEvent.data.orderId, ...extra }
  };

  await db.query('UPDATE orders SET status = $2, updated_at = now() WHERE id = $1', [sourceEvent.data.orderId, status]);
  await db.query(
    'INSERT INTO order_events (id, order_id, event_type, event_data) VALUES ($1, $2, $3, $4)',
    [eventId, sourceEvent.data.orderId, eventType, JSON.stringify(payload)]
  );
  await db.query(
    'INSERT INTO outbox_events (id, aggregate_id, event_type, topic, payload, traceparent) VALUES ($1, $2, $3, $4, $5, $6)',
    [eventId, sourceEvent.data.orderId, eventType, topic, JSON.stringify(payload), telemetry.traceparent]
  );
  return payload;
}

async function claimEvent(db, event) {
  const result = await db.query(
    'INSERT INTO processed_events (event_id, consumer_name) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING event_id',
    [event.eventId, groupId]
  );
  return result.rowCount === 1;
}

async function handleOrderCreated(db, event) {
  const items = event.data?.items;
  if (!event.data?.orderId || !Array.isArray(items) || items.length === 0) throw new Error('OrderCreated payload is invalid');
  const ids = items.map((item) => item.productId);
  const products = await db.query(
    'SELECT id, available_quantity FROM products WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE',
    [ids]
  );
  const stock = new Map(products.rows.map((product) => [product.id, product.available_quantity]));
  const rejected = items.find((item) => !stock.has(item.productId) || stock.get(item.productId) < item.quantity);

  if (rejected) {
    await addTransition(db, event, 'InventoryRejected', 'inventory.rejected', 'INVENTORY_REJECTED', {
      reason: 'INSUFFICIENT_STOCK',
      productId: rejected.productId
    });
    return;
  }

  for (const item of items) {
    await db.query(
      'UPDATE products SET available_quantity = available_quantity - $2, updated_at = now() WHERE id = $1',
      [item.productId, item.quantity]
    );
  }
  await addTransition(db, event, 'InventoryReserved', 'inventory.reserved', 'INVENTORY_RESERVED');
}

async function handleInventoryReserved(db, event) {
  if (Math.random() < paymentFailureRate) {
    await addTransition(db, event, 'PaymentRejected', 'payments.rejected', 'PAYMENT_REJECTED', {
      reason: 'SIMULATED_PAYMENT_REJECTION'
    });
    return;
  }
  await addTransition(db, event, 'PaymentApproved', 'payments.approved', 'PAYMENT_APPROVED');
}

async function recordTerminalMetric(db, orderId, outcome) {
  const result = await db.query('SELECT extract(epoch FROM (now() - created_at)) AS seconds FROM orders WHERE id = $1', [orderId]);
  ordersProcessed.inc({ outcome });
  if (result.rows[0]) processingDuration.observe({ outcome }, Number(result.rows[0].seconds));
}

async function handlePaymentApproved(db, event) {
  await addTransition(db, event, 'OrderCompleted', 'orders.completed', 'COMPLETED');
  await recordTerminalMetric(db, event.data.orderId, 'completed');
}

async function handleRejected(db, event) {
  await addTransition(db, event, 'OrderFailed', 'orders.failed', 'FAILED', {
    reason: event.data.reason ?? event.eventType
  });
  await recordTerminalMetric(db, event.data.orderId, 'failed');
}

const handlers = new Map([
  ['orders.created', handleOrderCreated],
  ['inventory.reserved', handleInventoryReserved],
  ['inventory.rejected', handleRejected],
  ['payments.approved', handlePaymentApproved],
  ['payments.rejected', handleRejected]
]);

async function processEvent(topic, event) {
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    if (!(await claimEvent(db, event))) {
      await db.query('COMMIT');
      logger.info({ eventId: event.eventId, orderId: event.data?.orderId, topic }, 'duplicate event ignored');
      return 'duplicate';
    }
    const handler = handlers.get(topic);
    if (!handler) throw new Error(`No handler registered for topic ${topic}`);
    await handler(db, event);
    await db.query('COMMIT');
    logger.info({ eventId: event.eventId, orderId: event.data?.orderId, topic }, 'event processed');
    return 'success';
  } catch (error) {
    await db.query('ROLLBACK').catch(() => undefined);
    throw error;
  } finally {
    db.release();
  }
}

function isUuid(value) {
  return typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

async function deadLetter(topic, key, rawValue, event, error, attempts) {
  const payload = {
    sourceTopic: topic,
    key,
    attempts,
    failedAt: new Date().toISOString(),
    error: error.message,
    originalEvent: event ?? rawValue
  };
  await producer.send({
    topic: 'order-processing.dlq',
    messages: [{ key: key ?? 'unknown', value: JSON.stringify(payload) }]
  });
  await pool.query(
    `INSERT INTO dead_letter_events (event_id, topic, event_key, payload, error_message, attempts)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [isUuid(event?.eventId) ? event.eventId : null, topic, key, JSON.stringify(payload), error.message, attempts]
  );
  deadLetters.inc({ source_topic: topic });
  logger.error({ err: error, topic, eventId: event?.eventId, orderId: event?.data?.orderId }, 'event moved to dead letter topic');
}

async function consumeMessage({ topic, message }) {
  const key = message.key?.toString() ?? null;
  const rawValue = message.value?.toString() ?? '';
  let event;
  try {
    event = JSON.parse(rawValue);
  } catch (error) {
    await deadLetter(topic, key, rawValue, null, error, 1);
    kafkaMessages.inc({ service, direction: 'consumed', topic, result: 'dead_letter' });
    return;
  }

  const carrier = {};
  for (const [header, value] of Object.entries(message.headers ?? {})) carrier[header] = value?.toString();
  const parent = propagation.extract(context.active(), carrier);

  await tracer.startActiveSpan(`process ${topic}`, {}, parent, async (span) => {
    span.setAttributes({ 'messaging.system': 'kafka', 'messaging.destination.name': topic, 'orderflow.order.id': event.data?.orderId ?? '' });
    let lastError;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        const result = await processEvent(topic, event);
        kafkaMessages.inc({ service, direction: 'consumed', topic, result });
        span.setAttribute('orderflow.processing.attempts', attempt);
        span.end();
        return;
      } catch (error) {
        lastError = error;
        logger.warn({ err: error, topic, eventId: event.eventId, attempt }, 'event processing attempt failed');
        if (attempt < 3) await sleep(250 * 2 ** (attempt - 1));
      }
    }

    span.recordException(lastError);
    span.setStatus({ code: SpanStatusCode.ERROR, message: lastError.message });
    await deadLetter(topic, key, rawValue, event, lastError, 3);
    kafkaMessages.inc({ service, direction: 'consumed', topic, result: 'dead_letter' });
    span.end();
  });
}

const server = http.createServer(async (request, response) => {
  if (request.url === '/metrics') {
    response.writeHead(200, { 'content-type': client.register.contentType });
    response.end(await client.register.metrics());
    return;
  }
  if (request.url === '/health/live') {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ status: 'ok', service }));
    return;
  }
  if (request.url === '/health/ready') {
    let postgres = false;
    try {
      await pool.query('SELECT 1');
      postgres = true;
    } catch {
      postgres = false;
    }
    const statusCode = ready && postgres ? 200 : 503;
    response.writeHead(statusCode, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ status: statusCode === 200 ? 'ready' : 'not_ready', kafka: ready, postgres }));
    return;
  }
  response.writeHead(404);
  response.end();
});

async function start() {
  await Promise.all([consumer.connect(), producer.connect()]);
  for (const topic of handlers.keys()) await consumer.subscribe({ topic, fromBeginning: false });
  await consumer.run({ eachMessage: consumeMessage });
  ready = true;
  server.listen(port, '0.0.0.0', () => logger.info({ port, groupId, paymentFailureRate }, 'worker listening'));
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  ready = false;
  logger.info({ signal }, 'shutting down');
  server.close();
  await Promise.allSettled([consumer.disconnect(), producer.disconnect(), pool.end()]);
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start().catch((error) => {
  logger.fatal({ err: error }, 'worker startup failed');
  process.exit(1);
});

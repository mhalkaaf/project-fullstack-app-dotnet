import http from 'node:http';

const baseUrl = process.env.ORDERFLOW_URL ?? 'http://127.0.0.1:8088';

function fetchJson(url, options = {}) {
  return new Promise((resolve, reject) => {
    const request = http.request(url, {
      method: options.method ?? 'GET',
      headers: options.headers ?? {}
    }, (response) => {
      let data = '';
      response.setEncoding('utf8');
      response.on('data', (chunk) => { data += chunk; });
      response.on('end', () => resolve({
        ok: response.statusCode >= 200 && response.statusCode < 300,
        status: response.statusCode,
        json: async () => JSON.parse(data || '{}')
      }));
    });
    request.on('error', reject);
    if (options.body) request.write(options.body);
    request.end();
  });
}

async function request(path, options) {
  const response = await fetchJson(`${baseUrl}${path}`, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`${path} returned ${response.status}: ${JSON.stringify(body)}`);
  return body;
}

const products = await request('/api/products');
if (!Array.isArray(products) || products.length === 0) throw new Error('No products returned');

const created = await request('/api/orders', {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({
    customerName: 'Smoke Test',
    items: [{ productId: products[0].id, quantity: 1 }]
  })
});

let order;
for (let attempt = 1; attempt <= 20; attempt += 1) {
  order = await request(`/api/orders/${created.id}`);
  process.stdout.write(`attempt=${attempt} order=${created.id} status=${order.status}\n`);
  if (['COMPLETED', 'FAILED'].includes(order.status)) break;
  await new Promise((resolve) => setTimeout(resolve, 1000));
}

if (!order || !['COMPLETED', 'FAILED'].includes(order.status)) {
  throw new Error(`Order did not reach a terminal state: ${order?.status}`);
}

process.stdout.write(`smoke test passed with ${order.events.length} events\n`);

const productsElement = document.querySelector('#products');
const catalogStatus = document.querySelector('#catalog-status');
const form = document.querySelector('#order-form');
const formMessage = document.querySelector('#form-message');
const submitButton = document.querySelector('#submit-order');
const emptyState = document.querySelector('#empty-state');
const orderDetail = document.querySelector('#order-detail');
const money = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' });
let products = [];
let pollTimer;

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

async function api(path, options) {
  const response = await fetch(path, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.error ?? `Request failed with status ${response.status}`);
  return payload;
}

async function loadProducts() {
  try {
    products = await api('/api/products');
    catalogStatus.textContent = `${products.length} products`;
    productsElement.innerHTML = products.map((product) => `
      <article class="product">
        <h3>${escapeHtml(product.name)}</h3>
        <span class="price">${money.format(Number(product.price))}</span>
        <p>${escapeHtml(product.description)}</p>
        <span class="stock">${product.available_quantity} in stock</span>
        <div class="quantity">
          <label for="product-${product.id}">Qty</label>
          <input id="product-${product.id}" data-product-id="${product.id}" type="number" min="0" max="${product.available_quantity}" value="0" />
        </div>
      </article>
    `).join('');
  } catch (error) {
    catalogStatus.textContent = 'Unavailable';
    productsElement.innerHTML = `<p>Could not load the catalog: ${escapeHtml(error.message)}</p>`;
  }
}

function renderOrder(order) {
  emptyState.classList.add('hidden');
  orderDetail.classList.remove('hidden');
  document.querySelector('#order-id').textContent = order.id;
  document.querySelector('#order-id').title = order.id;
  document.querySelector('#order-status').textContent = order.status.replaceAll('_', ' ');
  document.querySelector('#order-customer').textContent = order.customer_name;
  document.querySelector('#order-total').textContent = money.format(Number(order.total_amount));
  document.querySelector('#event-list').innerHTML = order.events.map((event) => `
    <li>
      <span class="event-name">${escapeHtml(event.event_type.replace(/([a-z])([A-Z])/g, '$1 $2'))}</span>
      <time class="event-time">${new Date(event.created_at).toLocaleString()}</time>
    </li>
  `).join('');
}

async function followOrder(orderId) {
  clearTimeout(pollTimer);
  try {
    const order = await api(`/api/orders/${orderId}`);
    renderOrder(order);
    if (!['COMPLETED', 'FAILED'].includes(order.status)) {
      pollTimer = setTimeout(() => followOrder(orderId), 1000);
    } else {
      await loadProducts();
    }
  } catch (error) {
    formMessage.textContent = `Tracking failed: ${error.message}`;
    pollTimer = setTimeout(() => followOrder(orderId), 2500);
  }
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  formMessage.textContent = '';
  const items = [...document.querySelectorAll('[data-product-id]')]
    .map((input) => ({ productId: input.dataset.productId, quantity: Number(input.value) }))
    .filter((item) => item.quantity > 0);
  if (items.length === 0) {
    formMessage.textContent = 'Choose at least one product.';
    return;
  }

  submitButton.disabled = true;
  submitButton.textContent = 'Publishing…';
  try {
    const order = await api('/api/orders', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ customerName: document.querySelector('#customer-name').value, items })
    });
    formMessage.textContent = '';
    await followOrder(order.id);
  } catch (error) {
    formMessage.textContent = error.message;
  } finally {
    submitButton.disabled = false;
    submitButton.innerHTML = 'Send order event <span aria-hidden="true">→</span>';
  }
});

loadProducts();

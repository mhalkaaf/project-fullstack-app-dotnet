"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { CreatedOrder, Order, OrderStatus, OrderSummary, Product } from "@/lib/types";

const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });
const dateTime = new Intl.DateTimeFormat("en-US", { dateStyle: "medium", timeStyle: "short" });

const statusCopy: Record<OrderStatus, { label: string; tone: string; detail: string }> = {
  PENDING: { label: "Accepted", tone: "active", detail: "The order and outbox event committed together." },
  INVENTORY_RESERVED: { label: "Inventory reserved", tone: "active", detail: "The worker confirmed stock and emitted the next event." },
  INVENTORY_REJECTED: { label: "Inventory rejected", tone: "danger", detail: "One or more products could not be reserved." },
  PAYMENT_APPROVED: { label: "Payment approved", tone: "active", detail: "The simulated payment step succeeded." },
  PAYMENT_REJECTED: { label: "Payment rejected", tone: "danger", detail: "The simulated payment step declined the order." },
  COMPLETED: { label: "Completed", tone: "success", detail: "Every asynchronous step completed successfully." },
  FAILED: { label: "Failed", tone: "danger", detail: "The workflow reached a terminal failure state." },
};

const eventCopy: Record<string, string> = {
  OrderCreated: "API committed the order and transactional outbox record.",
  InventoryReserved: "Worker reserved product inventory.",
  InventoryRejected: "Worker could not reserve the requested inventory.",
  PaymentApproved: "Simulated payment was approved.",
  PaymentRejected: "Simulated payment was rejected.",
  OrderCompleted: "The workflow reached its successful terminal state.",
  OrderFailed: "The workflow reached its failure terminal state.",
};

const terminalStatuses = new Set<OrderStatus>(["COMPLETED", "FAILED"]);

async function api<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(path, { ...options, cache: "no-store" });
  const payload = (await response.json().catch(() => ({}))) as { error?: string };
  if (!response.ok) throw new Error(payload.error ?? `Request failed with status ${response.status}`);
  return payload as T;
}

function friendlyEventName(value: string) {
  return value.replace(/([a-z])([A-Z])/g, "$1 $2");
}

export function OrderDashboard() {
  const [products, setProducts] = useState<Product[]>([]);
  const [recentOrders, setRecentOrders] = useState<OrderSummary[]>([]);
  const [quantities, setQuantities] = useState<Record<string, number>>({});
  const [customerName, setCustomerName] = useState("");
  const [activeOrderId, setActiveOrderId] = useState<string>();
  const [activeOrder, setActiveOrder] = useState<Order>();
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [notice, setNotice] = useState<{ tone: "error" | "info"; message: string }>();

  const loadProducts = useCallback(async () => {
    setCatalogLoading(true);
    try {
      setProducts(await api<Product[]>("/api/products"));
    } catch (error) {
      setNotice({ tone: "error", message: error instanceof Error ? error.message : "Could not load products." });
    } finally {
      setCatalogLoading(false);
    }
  }, []);

  const loadRecentOrders = useCallback(async () => {
    try {
      setRecentOrders(await api<OrderSummary[]>("/api/orders"));
    } catch {
      // Ordering still works if the optional recent-order panel cannot load.
    }
  }, []);

  useEffect(() => {
    void Promise.all([loadProducts(), loadRecentOrders()]);
  }, [loadProducts, loadRecentOrders]);

  useEffect(() => {
    if (!activeOrderId || (activeOrder && terminalStatuses.has(activeOrder.status))) return;
    let cancelled = false;

    const syncOrder = async () => {
      try {
        const order = await api<Order>(`/api/orders/${activeOrderId}`);
        if (cancelled) return;
        setActiveOrder(order);
        if (terminalStatuses.has(order.status)) {
          setNotice({ tone: "info", message: `Order ${statusCopy[order.status].label.toLowerCase()}. The catalog is refreshed.` });
          await Promise.all([loadProducts(), loadRecentOrders()]);
        }
      } catch (error) {
        if (!cancelled) {
          setNotice({ tone: "error", message: `Tracking paused: ${error instanceof Error ? error.message : "unknown error"}` });
        }
      }
    };

    void syncOrder();
    const timer = window.setInterval(() => void syncOrder(), 1000);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [activeOrderId, activeOrder?.status, loadProducts, loadRecentOrders]);

  const selectedItems = useMemo(
    () => products.filter((product) => (quantities[product.id] ?? 0) > 0),
    [products, quantities],
  );
  const itemCount = selectedItems.reduce((sum, product) => sum + (quantities[product.id] ?? 0), 0);
  const orderTotal = selectedItems.reduce(
    (sum, product) => sum + Number(product.price) * (quantities[product.id] ?? 0),
    0,
  );

  function setQuantity(product: Product, nextValue: number) {
    const value = Math.max(0, Math.min(product.available_quantity, nextValue));
    setQuantities((current) => ({ ...current, [product.id]: value }));
    setNotice(undefined);
  }

  async function submitOrder(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const name = customerName.trim();
    if (!name) {
      setNotice({ tone: "error", message: "Add a customer name before publishing the order." });
      return;
    }
    if (selectedItems.length === 0) {
      setNotice({ tone: "error", message: "Select at least one product to continue." });
      return;
    }

    setSubmitting(true);
    setNotice({ tone: "info", message: "Committing the order and its first outbox event…" });
    try {
      const created = await api<CreatedOrder>("/api/orders", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          customerName: name,
          items: selectedItems.map((product) => ({ productId: product.id, quantity: quantities[product.id] })),
        }),
      });
      setActiveOrder(undefined);
      setActiveOrderId(created.id);
      setQuantities({});
      setNotice({ tone: "info", message: "Order accepted. Following its Kafka events live…" });
      await loadRecentOrders();
    } catch (error) {
      setNotice({ tone: "error", message: error instanceof Error ? error.message : "The order could not be created." });
    } finally {
      setSubmitting(false);
    }
  }

  function inspectOrder(orderId: string) {
    setActiveOrder(undefined);
    setActiveOrderId(orderId);
    setNotice({ tone: "info", message: "Loading the selected order timeline…" });
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  const activeStatus = activeOrder ? statusCopy[activeOrder.status] : undefined;

  return (
    <main>
      <header className="topbar">
        <a className="brand" href="#top" aria-label="OrderFlow home">
          <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
          <span>OrderFlow</span>
        </a>
        <nav className="ops-nav" aria-label="Observability tools">
          <span className="system-live"><i /> local stack</span>
          <a href="http://localhost:3009" target="_blank" rel="noreferrer">Grafana <span>↗</span></a>
          <a href="http://localhost:8089" target="_blank" rel="noreferrer">Kafka UI <span>↗</span></a>
          <a href="http://localhost:9098" target="_blank" rel="noreferrer">Prometheus <span>↗</span></a>
        </nav>
      </header>

      <section className="hero" id="top">
        <div className="hero-copy">
          <p className="kicker">EVENT-DRIVEN SYSTEMS LAB</p>
          <h1>Place an order.<br /><em>Watch the system think.</em></h1>
          <p className="hero-lede">
            A hands-on path through Next.js, .NET 10, PostgreSQL, Kafka, and OpenTelemetry—one observable event at a time.
          </p>
        </div>
        <div className="flow-map" aria-label="OrderFlow architecture summary">
          {["Next.js", ".NET API", "Outbox", "Kafka", "Worker"].map((step, index) => (
            <div className="flow-node" key={step}>
              <span>{String(index + 1).padStart(2, "0")}</span>
              <strong>{step}</strong>
              {index < 4 && <b aria-hidden="true">→</b>}
            </div>
          ))}
        </div>
      </section>

      <div className="workspace">
        <section className="builder" aria-labelledby="builder-heading">
          <div className="section-title">
            <div>
              <p className="step-label"><span>01</span> Build</p>
              <h2 id="builder-heading">Choose what to send</h2>
              <p>Select products and quantities. Inventory is validated asynchronously by the worker.</p>
            </div>
            <span className="count-badge">{catalogLoading ? "Loading" : `${products.length} products`}</span>
          </div>

          <form onSubmit={submitOrder}>
            <div className="product-grid" aria-busy={catalogLoading}>
              {catalogLoading && [1, 2, 3, 4].map((item) => <div className="product-card skeleton" key={item} />)}
              {!catalogLoading && products.map((product, index) => {
                const quantity = quantities[product.id] ?? 0;
                return (
                  <article className={`product-card ${quantity > 0 ? "selected" : ""}`} key={product.id}>
                    <div className="product-index">{String(index + 1).padStart(2, "0")}</div>
                    <div className="product-copy">
                      <div className="product-heading">
                        <h3>{product.name}</h3>
                        <strong>{money.format(Number(product.price))}</strong>
                      </div>
                      <p>{product.description}</p>
                    </div>
                    <div className="stock-line">
                      <span><i className={product.available_quantity > 0 ? "in-stock" : "out-stock"} />{product.available_quantity} available</span>
                      <div className="stepper" aria-label={`${product.name} quantity`}>
                        <button type="button" onClick={() => setQuantity(product, quantity - 1)} disabled={quantity === 0} aria-label={`Remove one ${product.name}`}>−</button>
                        <output aria-live="polite">{quantity}</output>
                        <button type="button" onClick={() => setQuantity(product, quantity + 1)} disabled={quantity >= product.available_quantity} aria-label={`Add one ${product.name}`}>+</button>
                      </div>
                    </div>
                  </article>
                );
              })}
            </div>

            <div className="checkout-card">
              <div className="customer-input">
                <label htmlFor="customer-name">Customer name</label>
                <input
                  id="customer-name"
                  value={customerName}
                  onChange={(event) => setCustomerName(event.target.value)}
                  maxLength={120}
                  placeholder="Ada Lovelace"
                  autoComplete="name"
                />
              </div>
              <div className="order-total">
                <span>{itemCount} {itemCount === 1 ? "item" : "items"}</span>
                <strong>{money.format(orderTotal)}</strong>
              </div>
              <button className="publish-button" disabled={submitting || selectedItems.length === 0} type="submit">
                <span>{submitting ? "Publishing event…" : "Publish order"}</span>
                <b aria-hidden="true">→</b>
              </button>
            </div>
            {notice && <p className={`notice ${notice.tone}`} role="status"><span>{notice.tone === "error" ? "!" : "i"}</span>{notice.message}</p>}
          </form>
        </section>

        <aside className="journey" aria-labelledby="journey-heading">
          <div className="journey-header">
            <div>
              <p className="step-label"><span>02</span> Observe</p>
              <h2 id="journey-heading">Live event journey</h2>
            </div>
            {activeStatus && <span className={`status-pill ${activeStatus.tone}`}><i />{activeStatus.label}</span>}
          </div>

          {!activeOrderId && (
            <div className="empty-journey">
              <div className="radar" aria-hidden="true"><i /><i /><i /></div>
              <h3>Waiting for an order</h3>
              <p>Your event timeline will appear here and update as Kafka moves work between services.</p>
              <div className="empty-hint"><span>Start here</span><b>Choose a product and publish →</b></div>
            </div>
          )}

          {activeOrderId && !activeOrder && (
            <div className="loading-journey">
              <div className="spinner" />
              <strong>Following {activeOrderId.slice(0, 8)}…</strong>
              <span>Waiting for the latest state</span>
            </div>
          )}

          {activeOrder && (
            <div className="order-detail">
              <div className="order-identity">
                <div>
                  <span>ORDER ID</span>
                  <button type="button" title={activeOrder.id} onClick={() => void navigator.clipboard?.writeText(activeOrder.id)}>{activeOrder.id.slice(0, 8)}…</button>
                </div>
                <div><span>TOTAL</span><strong>{money.format(Number(activeOrder.total_amount))}</strong></div>
              </div>
              <div className="status-explainer">
                <span className={`signal ${activeStatus?.tone}`} />
                <div><strong>{activeStatus?.label}</strong><p>{activeStatus?.detail}</p></div>
              </div>
              <ol className="timeline">
                {activeOrder.events.map((orderEvent, index) => (
                  <li key={orderEvent.id} className={index === activeOrder.events.length - 1 ? "latest" : ""}>
                    <div className="event-marker"><span>{index + 1}</span></div>
                    <div className="event-card">
                      <div><strong>{friendlyEventName(orderEvent.event_type)}</strong><time>{dateTime.format(new Date(orderEvent.created_at))}</time></div>
                      <p>{eventCopy[orderEvent.event_type] ?? "The event was recorded in the order workflow."}</p>
                    </div>
                  </li>
                ))}
              </ol>
              {!terminalStatuses.has(activeOrder.status) && <p className="polling"><i />Listening for the next Kafka event</p>}
            </div>
          )}
        </aside>
      </div>

      <section className="recent-section" aria-labelledby="recent-heading">
        <div className="section-title compact">
          <div><p className="step-label"><span>03</span> Revisit</p><h2 id="recent-heading">Recent orders</h2></div>
          <button type="button" className="text-button" onClick={() => void loadRecentOrders()}>Refresh list ↻</button>
        </div>
        {recentOrders.length === 0 ? (
          <p className="no-orders">No orders yet. Your first published order will appear here.</p>
        ) : (
          <div className="recent-grid">
            {recentOrders.slice(0, 8).map((order) => {
              const copy = statusCopy[order.status];
              return (
                <button type="button" className={`recent-order ${activeOrderId === order.id ? "current" : ""}`} key={order.id} onClick={() => inspectOrder(order.id)}>
                  <span className={`status-dot ${copy.tone}`} />
                  <span><strong>{order.customer_name}</strong><small>{order.id.slice(0, 8)} · {dateTime.format(new Date(order.created_at))}</small></span>
                  <span className="recent-total"><strong>{money.format(Number(order.total_amount))}</strong><small>{copy.label}</small></span>
                  <b aria-hidden="true">→</b>
                </button>
              );
            })}
          </div>
        )}
      </section>

      <footer>
        <div><span className="brand-mark small" aria-hidden="true"><i /><i /><i /></span><strong>OrderFlow</strong></div>
        <p>Next.js · .NET 10 · PostgreSQL · Kafka · OpenTelemetry</p>
        <p>Built to make distributed systems visible.</p>
      </footer>
    </main>
  );
}

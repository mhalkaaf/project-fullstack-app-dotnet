export type Product = {
  id: string;
  name: string;
  description: string;
  price: number;
  available_quantity: number;
};

export type OrderEvent = {
  id: string;
  event_type: string;
  created_at: string;
};

export type Order = {
  id: string;
  customer_name: string;
  total_amount: number;
  status: OrderStatus;
  created_at: string;
  updated_at: string;
  events: OrderEvent[];
};

export type OrderSummary = Omit<Order, "events">;

export type OrderStatus =
  | "PENDING"
  | "INVENTORY_RESERVED"
  | "INVENTORY_REJECTED"
  | "PAYMENT_APPROVED"
  | "PAYMENT_REJECTED"
  | "COMPLETED"
  | "FAILED";

export type CreatedOrder = {
  id: string;
  status: OrderStatus;
  total_amount: number;
};

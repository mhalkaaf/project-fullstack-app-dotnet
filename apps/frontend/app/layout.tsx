import type { Metadata } from "next";
import type { ReactNode } from "react";
import "./globals.css";

export const metadata: Metadata = {
  title: "OrderFlow | Event-driven systems lab",
  description: "Create an order and follow it through .NET, PostgreSQL, Kafka, and OpenTelemetry.",
};

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

import type { NextRequest } from "next/server";

type RouteContext = { params: Promise<{ path: string[] }> };

async function proxy(request: NextRequest, context: RouteContext) {
  const { path } = await context.params;
  const baseUrl = process.env.API_INTERNAL_URL ?? "http://127.0.0.1:3008";
  const target = new URL(`/api/${path.join("/")}`, baseUrl);
  target.search = request.nextUrl.search;

  const headers = new Headers();
  const contentType = request.headers.get("content-type");
  if (contentType) headers.set("content-type", contentType);
  const traceparent = request.headers.get("traceparent");
  if (traceparent) headers.set("traceparent", traceparent);

  try {
    const upstream = await fetch(target, {
      method: request.method,
      headers,
      body: request.method === "GET" || request.method === "HEAD" ? undefined : await request.arrayBuffer(),
      cache: "no-store",
      signal: request.signal,
    });

    const responseHeaders = new Headers();
    const upstreamContentType = upstream.headers.get("content-type");
    if (upstreamContentType) responseHeaders.set("content-type", upstreamContentType);
    responseHeaders.set("x-orderflow-proxy", "nextjs");

    return new Response(upstream.body, {
      status: upstream.status,
      headers: responseHeaders,
    });
  } catch {
    return Response.json(
      { error: "The OrderFlow API is currently unavailable. Please try again shortly." },
      { status: 503 },
    );
  }
}

export { proxy as GET, proxy as POST, proxy as OPTIONS };

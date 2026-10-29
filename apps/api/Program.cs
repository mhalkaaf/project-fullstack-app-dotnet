using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using OrderFlow.Api;
using OrderFlow.Shared;

var builder = WebApplication.CreateBuilder(args);
builder.AddOrderFlowDefaults("orderflow-api");
builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 1_000_000);
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower;
    options.SerializerOptions.DictionaryKeyPolicy = JsonNamingPolicy.SnakeCaseLower;
});
builder.Services.AddCors(options => options.AddDefaultPolicy(policy =>
    policy.AllowAnyOrigin().AllowAnyHeader().WithMethods("GET", "POST", "OPTIONS")));
builder.Services.AddSingleton<OrderRepository>();

var app = builder.Build();
app.UseCors();
app.Use(async (context, next) =>
{
    var started = Stopwatch.GetTimestamp();
    try
    {
        await next(context);
    }
    catch (ApiException exception)
    {
        context.Response.StatusCode = exception.StatusCode;
        await context.Response.WriteAsJsonAsync(new { error = exception.Message });
    }
    catch (Exception exception)
    {
        context.Response.StatusCode = 500;
        app.Logger.LogError(exception, "Request {Method} {Path} failed", context.Request.Method, context.Request.Path);
        await context.Response.WriteAsJsonAsync(new { error = "Internal server error" });
    }
    finally
    {
        var route = RouteName(context.Request.Method, context.Request.Path);
        var tags = new TagList
        {
            { "method", context.Request.Method },
            { "route", route },
            { "status_code", context.Response.StatusCode.ToString(System.Globalization.CultureInfo.InvariantCulture) }
        };
        Telemetry.HttpRequests.Add(1, tags);
        Telemetry.HttpDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds,
            new KeyValuePair<string, object?>("method", context.Request.Method),
            new KeyValuePair<string, object?>("route", route));
    }
});

app.MapOrderFlowHealth(requireConsumer: false);
app.MapGet("/api/products", (OrderRepository repository, CancellationToken token) => repository.GetProducts(token));
app.MapGet("/api/orders", (OrderRepository repository, CancellationToken token) => repository.GetOrders(token));
app.MapGet("/api/orders/{orderId:guid}", async (Guid orderId, OrderRepository repository, CancellationToken token) =>
{
    var order = await repository.GetOrder(orderId, token);
    return order is null ? Results.NotFound(new { error = "Order not found" }) : Results.Ok(order);
});
app.MapPost("/api/orders", async (CreateOrderRequest request, OrderRepository repository, CancellationToken token) =>
    Results.Accepted(value: await repository.CreateOrder(request, token)));

app.Run();

static string RouteName(string method, PathString path)
{
    if (path == "/api/products") return $"{method} /api/products";
    if (path == "/api/orders") return $"{method} /api/orders";
    if (path.StartsWithSegments("/api/orders")) return $"{method} /api/orders/:id";
    if (path.StartsWithSegments("/health")) return $"{method} /health/:kind";
    return $"{method} unmatched";
}

public partial class Program;

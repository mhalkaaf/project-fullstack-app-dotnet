using System.Diagnostics;
using Dapper;
using Microsoft.Extensions.Logging;
using Npgsql;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace OrderFlow.Shared;

public static class HostingExtensions
{
    public static WebApplicationBuilder AddOrderFlowDefaults(this WebApplicationBuilder builder, string serviceName)
    {
        DefaultTypeMap.MatchNamesWithUnderscores = true;
        var options = OrderFlowOptions.FromConfiguration(builder.Configuration);

        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel((builder.Configuration["LOG_LEVEL"] ?? "info").ToLowerInvariant() switch
        {
            "trace" => LogLevel.Trace,
            "debug" => LogLevel.Debug,
            "warning" or "warn" => LogLevel.Warning,
            "error" => LogLevel.Error,
            "critical" or "fatal" => LogLevel.Critical,
            "none" => LogLevel.None,
            _ => LogLevel.Information
        });
        builder.Logging.Configure(logging => logging.ActivityTrackingOptions =
            ActivityTrackingOptions.TraceId | ActivityTrackingOptions.SpanId | ActivityTrackingOptions.ParentId);
        builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
        builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Information);
        builder.Logging.AddJsonConsole(console => console.IncludeScopes = true);

        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(options.DatabaseUrl));
        builder.Services.AddSingleton<KafkaState>();
        builder.Services.AddSingleton<KafkaProducer>();
        builder.Services.AddHostedService<OutboxPublisher>();

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(serviceName))
            .WithTracing(tracing => tracing
                .AddSource(Telemetry.ActivitySourceName)
                .AddAspNetCoreInstrumentation(instrumentation =>
                    instrumentation.Filter = context => !context.Request.Path.StartsWithSegments("/health"))
                .AddNpgsql()
                .AddOtlpExporter())
            .WithMetrics(metrics => metrics
                .AddMeter(Telemetry.MeterName)
                .AddAspNetCoreInstrumentation()
                .AddRuntimeInstrumentation()
                .AddOtlpExporter());

        return builder;
    }

    public static void MapOrderFlowHealth(this WebApplication app, bool requireConsumer)
    {
        app.MapGet("/health/live", (IHostEnvironment environment) => Results.Ok(new
        {
            status = "ok",
            service = environment.ApplicationName
        })).ExcludeFromDescription();

        app.MapGet("/health/ready", async (NpgsqlDataSource dataSource, KafkaState kafka, CancellationToken token) =>
        {
            var postgres = false;
            try
            {
                await using var command = dataSource.CreateCommand("SELECT 1");
                await command.ExecuteScalarAsync(token);
                postgres = true;
            }
            catch (Exception)
            {
                // The response communicates dependency state without leaking connection details.
            }

            var kafkaReady = kafka.ProducerReady && (!requireConsumer || kafka.ConsumerReady);
            return postgres && kafkaReady
                ? Results.Ok(new { status = "ready", postgres, kafka = true })
                : Results.Json(new { status = "not_ready", postgres, kafka = kafkaReady }, statusCode: 503);
        }).ExcludeFromDescription();
    }
}

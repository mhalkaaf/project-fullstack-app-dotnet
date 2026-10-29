using OrderFlow.Shared;
using OrderFlow.Worker;

var builder = WebApplication.CreateBuilder(args);
builder.AddOrderFlowDefaults("orderflow-worker");
builder.Services.AddSingleton<OrderEventProcessor>();
builder.Services.AddHostedService<KafkaConsumerService>();

var app = builder.Build();
app.MapOrderFlowHealth(requireConsumer: true);
app.Run();

public partial class Program;

namespace OrderFlow.Shared;

public sealed class KafkaState
{
    private int _producerReady;
    private int _consumerReady;

    public bool ProducerReady
    {
        get => Volatile.Read(ref _producerReady) == 1;
        set => Interlocked.Exchange(ref _producerReady, value ? 1 : 0);
    }

    public bool ConsumerReady
    {
        get => Volatile.Read(ref _consumerReady) == 1;
        set => Interlocked.Exchange(ref _consumerReady, value ? 1 : 0);
    }
}

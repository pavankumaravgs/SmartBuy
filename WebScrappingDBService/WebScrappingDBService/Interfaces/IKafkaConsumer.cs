namespace WebScrappingDBService.Interfaces
{
    public interface IKafkaConsumer
    {
    void Subscribe(string topic);
    string? ConsumeMessage();
    void Commit();
    void Close();
    void ProcessMessages(Func<string, Task<bool>> handleMessage);
    }
}

using System.Collections.Concurrent;

namespace LightweightActors.Tests;

public class JobDispatcherTests
{
    [Fact]
    public async Task Message_AreProcessed_InPostingOrder()
    {
        var dispatcher = new JobDispatcher();
        var processed = new ConcurrentQueue<int>();
        
        Task[] completions = Enumerable.Range(0, 100)
            .Select(i => dispatcher.Post(() => processed.Enqueue(i)))
            .ToArray();
        
        await Task.WhenAll(completions);
        
        Assert.Equal(Enumerable.Range(0, 100), processed);
        Assert.Equal(0, dispatcher.PendingCount);
    }
}
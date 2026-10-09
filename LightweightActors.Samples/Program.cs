using System.Collections.Concurrent;
using System.Diagnostics;
using LightweightActors;

const int DispatcherCount = 4;
const int MessagesPerDispatcher = 100;

await RunExperiment("① sync  / 1 producer",  useAsync: false, multiProducer: false);
await RunExperiment("② sync  / 4 producers", useAsync: false, multiProducer: true);
await RunExperiment("③ async / 1 producer",  useAsync: true,  multiProducer: false);
await RunExperiment("④ async / 4 producers", useAsync: true,  multiProducer: true);
async Task RunExperiment(string name, bool useAsync, bool multiProducer)
{
    var records = new ConcurrentQueue<(int Dispatcher, string Phase, int ThreadId)>();
    var dispatchers = new List<JobDispatcher>();
    for (int i = 0; i < DispatcherCount; i++)
    {
        dispatchers.Add(new JobDispatcher());
    }
    
    // 디스패처 d 하나에 메시지를 MessagesPerDispatcher개 Post하고, Post가 돌려준 Task들을 반환
    Task[] Produce(int d)
    {
        var completions = new Task[MessagesPerDispatcher];
        for (int i = 0; i < MessagesPerDispatcher; i++)
        {
            records.Enqueue((d, "caller", Thread.CurrentThread.ManagedThreadId));
            
            if (useAsync)
            {
                completions[i] = dispatchers[d].Post((records, d), async static s =>
                {
                    s.records.Enqueue((s.d, "start", Thread.CurrentThread.ManagedThreadId));
                    await Task.Delay(1);
                    s.records.Enqueue((s.d, "after", Thread.CurrentThread.ManagedThreadId));
                });
            }
            else
            {
                completions[i] = dispatchers[d].Post((records, d), static s =>
                {
                    s.records.Enqueue((s.d, "start", Thread.CurrentThread.ManagedThreadId));
                    Thread.Sleep(1);
                });

            }
        }
        return completions;
    }
    
    var sw = Stopwatch.StartNew();
    Task[] all;
    if (multiProducer)
    {
        var tasks = Enumerable.Range(0, dispatchers.Count)
            .Select(d => Task.Run(() => Produce(d)))
            .ToArray();
        var allTasks = await Task.WhenAll(tasks);
        all = allTasks.SelectMany(t => t).ToArray();
    }
    else
    {
        all = Enumerable.Range(0, dispatchers.Count)
            .Select(Produce)
            .SelectMany(t => t)
            .ToArray();
    }
    await Task.WhenAll(all);
    sw.Stop();

    Print(name, sw.Elapsed, records);
}

static void Print(string name, TimeSpan elapsed,
    IEnumerable<(int Dispatcher, string Phase, int ThreadId)> records)
{
    Console.WriteLine($"[{name}] elapsed={elapsed.TotalMilliseconds:F0}ms");
    
    var callerThreads = records.Where(r => r.Phase == "caller").Select(r => r.ThreadId).Distinct().ToArray();
    var startThreads = records.Where(r => r.Phase == "start").Select(r => r.ThreadId).Distinct().ToArray();
    var afterThreads = records.Where(r => r.Phase == "after").Select(r => r.ThreadId).Distinct().ToArray();
    var callerInStart = startThreads.Intersect(callerThreads).ToArray();
    
    Console.WriteLine($"caller thread is start thread: {callerInStart.Length} / {callerThreads.Length}");
    Console.WriteLine($"caller threads: {string.Join(", ", callerThreads)}");
    Console.WriteLine($"start threads: {string.Join(", ", startThreads)}");
    Console.WriteLine($"after threads: {string.Join(", ", afterThreads)}");
    
    foreach (var d in Enumerable.Range(0, DispatcherCount))
    {
        var startThreadsOfDispatcher = records.Where(r => r.Dispatcher == d && r.Phase == "start").Select(r => r.ThreadId).Distinct().ToArray();
        Console.WriteLine($"dispatcher {d} start threads: {string.Join(", ", startThreadsOfDispatcher)}");
    }
}
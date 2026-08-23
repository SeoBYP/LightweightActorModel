using System.Collections.Concurrent;

namespace LightweightActors;

public class JobScheduler
{
    private static readonly ConcurrentQueue<JobDispatcher?> ReadyQueue = new();
    private static readonly int MaxWorkerCount = Math.Max(1, Environment.ProcessorCount);
    private static int _workerCount;

    public static void Schedule(JobDispatcher dispatcher)
    {
        ReadyQueue.Enqueue(dispatcher);
        EnsureWorker();
    }

    private static void EnsureWorker()
    {
        while (true)
        {
            int workerCount = Volatile.Read(ref _workerCount);
            if (workerCount > MaxWorkerCount)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _workerCount, workerCount + 1, workerCount) == workerCount)
            {
                try
                {
                    while (ReadyQueue.TryDequeue(out JobDispatcher? jobDispatcher))
                    {
                        jobDispatcher?.ProcessNext();
                    }
                    
                    if(ReadyQueue.IsEmpty)
                    {
                        return;
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref _workerCount);
                    if(!ReadyQueue.IsEmpty)
                    {
                        EnsureWorker();
                    }
                }
            }
        }
    }
}
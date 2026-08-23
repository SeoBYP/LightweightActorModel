using System.Collections.Concurrent;

namespace LightweightActors;

public class JobDispatcher
{
    // 현재 비동기 실행 디스패쳐를 추적
    private static readonly AsyncLocal<JobDispatcher> CurrentDispatcher = new();
    
    // 여러 생산자가 동시에 메세지를 넣을 수 있는 큐
    private readonly ConcurrentQueue<IMessage> _jobQueue = new();

    // 현재 실행 중인 메시지를 포함한 미왼료인 메시지 수
    private int _jobCount;

    // 이 디스패쳐가 실행 권한을 가지고 있는 지 나타내는 값
    private int _scheduled;

    public int PendingCount => Volatile.Read(ref _jobCount);

    public Task Post(Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return Enqueue(new SyncMessage(action));
    }

    public Task Post(Func<Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return Enqueue(new AsyncMessage(action));
    }

    private Task Enqueue(IMessage message)
    {
        _jobQueue.Enqueue(message);
        Interlocked.Increment(ref _jobCount);

        if (Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0)
        {
            // 스케줄러에게 실행 요청
            JobScheduler.Schedule(this);
        }

        return message.Completion;
    }

    internal void ProcessNext()
    {
        if(!_jobQueue.TryDequeue(out IMessage? message))
        {
            DeactivateOrReschedule();
            return;
        }

        JobDispatcher? provious = CurrentDispatcher.Value;
        CurrentDispatcher.Value = this;

        try
        {
            ValueTask execution = message.ExecuteAsync();
            if (execution.IsCompleted)
            {
                CompleteSynchronously(message, execution);
            }
            else
            {
                _ = CompleteAsynchronously(message, execution);
            }
        }
        catch (Exception e)
        {
            message.SetException(e);
            FinishMessage();
        }
        finally
        {
            CurrentDispatcher.Value = provious;
        }
    }

    private void CompleteSynchronously(IMessage message, ValueTask execution)
    {
        try
        {
            execution.GetAwaiter().GetResult();
            message.SetResult();
        }
        catch (Exception e)
        {
            message.SetException(e);
        }

        FinishMessage();
    }

    private async Task CompleteAsynchronously(IMessage message, ValueTask execution)
    {
        try
        {
            await execution.ConfigureAwait(false);
            message.SetResult();
        }
        catch (Exception e)
        {
            message.SetException(e);
        }
        
        FinishMessage();
    }

    private void FinishMessage()
    {
        if (Interlocked.Decrement(ref _jobCount) > 0)
        {
            JobScheduler.Schedule(this);
        }
        else
        {
            DeactivateOrReschedule();
        }
    }

    private void DeactivateOrReschedule()
    {
        Volatile.Write(ref _scheduled, 0);
        // 메시지 수가 0이 된 직후 디스패치가 유휴 상태로 바뀌기 전에 새 메시지가 들어올 수 있음
        // 이런 레이스가 발생한 경우 실행 권한을 다시 획득
        if(Volatile.Read(ref _jobCount) > 0 && Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0)
        {
            JobScheduler.Schedule(this);
        }
    }
    
}
using System.Collections.Concurrent;

namespace LightweightActors;

public class JobDispatcher
{
    private const long MessageCountMask = uint.MaxValue;
    private const long ClosingFlag = 1L << 32;
    private const long ClosedFlag = 1L << 33;
    
    // 현재 비동기 실행 디스패쳐를 추적
    private static readonly AsyncLocal<JobDispatcher> CurrentDispatcher = new();
    
    // 여러 생산자가 동시에 메세지를 넣을 수 있는 큐
    private readonly ConcurrentQueue<IMessage> _jobQueue = new();
    private readonly TaskCompletionSource<bool> _closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // 현재 실행 중인 메시지를 포함한 미왼료인 메시지 수
    // private int _jobCount;

    // 이 디스패쳐가 실행 권한을 가지고 있는 지 나타내는 값
    private int _scheduled;

    private long _lifecycle;
    
    // 이 디스패쳐가 종료되었는지 나타내는 값
    public Task Closed => _closed.Task;
    
    public int PendingCount => (int)(Volatile.Read(ref _lifecycle) & MessageCountMask);

    public ActorState State
    {
        get
        {
            long lifecycle = Volatile.Read(ref _lifecycle);
            if((lifecycle & ClosedFlag) != 0)
                return ActorState.Closed;
            return (lifecycle & ClosingFlag) != 0 ? ActorState.Closing : ActorState.Active;
        }
    }


    public static JobDispatcher? Current => CurrentDispatcher.Value;
    
    
    
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

    public Task Post<TState>(TState state, Action<TState> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return Enqueue(new SyncMessage<TState>(state, action));
    }
    
    public Task Post<TState>(TState state,Func<TState,Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return Enqueue(new AsyncMessage<TState>(state,action));
    }

    private Task Enqueue(IMessage message)
    {
        while (true)
        {
            long current = Volatile.Read(ref _lifecycle);
            if ((current & (ClosingFlag | ClosedFlag)) != 0) 
                throw new InvalidOperationException("Actor is closed");
            
            long next = current + 1; 
            
            // _lifecycle이 
            if (Interlocked.CompareExchange(ref _lifecycle, next, current) == current)
                break;
        }
        
        _jobQueue.Enqueue(message);
        
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
        long after = Interlocked.Decrement(ref _lifecycle);
        
        if ((after & MessageCountMask) > 0)
        {
            JobScheduler.Schedule(this);
        }
        else
        {
            if ((after & ClosingFlag) != 0)
            {
                TryMarkClosed();
            }
            DeactivateOrReschedule();
        }
    }

    private void DeactivateOrReschedule()
    {
        Volatile.Write(ref _scheduled, 0);
        // 메시지 수가 0이 된 직후 디스패치가 유휴 상태로 바뀌기 전에 새 메시지가 들어올 수 있음
        // 이런 레이스가 발생한 경우 실행 권한을 다시 획득
        if(PendingCount > 0 && Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0)
        {
            JobScheduler.Schedule(this);
        }
    }

    public void RequestClose()
    {
        while (true)
        {
            long lifecycle = Volatile.Read(ref _lifecycle);
            // & 은 | 보다 연산자 우선순위가 높아서 
            // (ClosingFlag | ClosedFlag) 으로 감싸야함
            if((lifecycle & (ClosingFlag | ClosedFlag)) != 0)
                return;

            long closing = lifecycle | ClosingFlag;
            
            if(Interlocked.CompareExchange(ref _lifecycle, closing, lifecycle) == lifecycle)
            {
                if ((closing & MessageCountMask) == 0)
                {
                    TryMarkClosed();
                }
                return;
            }
        }
    }

    private void TryMarkClosed()
    {
        if (Interlocked.CompareExchange(ref _lifecycle, ClosedFlag, ClosingFlag) == ClosingFlag)
        {
            _closed.SetResult(true);
        }
    }
    
    // 숙제 : Closed, Closing 상태에 따른 메시지 예외 처리
}
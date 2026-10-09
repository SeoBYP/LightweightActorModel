using System.Collections.Concurrent;

namespace LightweightActors.Tests;

public class JobDispatcherTests
{
    [Fact]
    public async Task 메시지는_Post한_순서대로_처리된다()
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

    [Fact]
    public async Task 비동기_메시지가_끝날때까지_같은_디스패처의_다음_메시지는_대기한다()
    {
        var dispatcher = new JobDispatcher();
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        bool secondStarted = false;

        Task first = dispatcher.Post(async () =>
        {
            firstStarted.SetResult();
            await releaseFirst.Task;
        });

        Task second = dispatcher.Post(() => secondStarted = true);

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(secondStarted);

        releaseFirst.SetResult();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(secondStarted);
    }

    [Fact]
    public async Task 한_디스패처가_await중이어도_다른_디스패처는_막히지_않는다()
    {
        var firstDispatcher = new JobDispatcher();
        var secondDispatcher = new JobDispatcher();

        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondRun = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = firstDispatcher.Post(() => releaseFirst.Task);
        var second = secondDispatcher.Post(secondRun.SetResult);

        await secondRun.Task.WaitAsync(TimeSpan.FromSeconds(5));
        releaseFirst.SetResult();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task 메시지에서_예외가_나도_Task로_전달되고_다음_메시지는_계속_처리된다()
    {
        var dispatcher = new JobDispatcher();
        bool laterMessageRun = false;

        Task failed = dispatcher.Post(() => throw new InvalidOperationException("Boom"));
        Task later = dispatcher.Post(() => laterMessageRun = true);

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(() => failed);
        await later.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("Boom", exception.Message);
        Assert.True(laterMessageRun);
    }

    [Fact]
    public async Task Current_디스패처는_await_이후에도_유지된다()
    {
        var dispatcher = new JobDispatcher();
        JobDispatcher? beforeAwait = null;
        JobDispatcher? afterAwait = null;

        await dispatcher.Post(async () =>
        {
            beforeAwait = JobDispatcher.Current;
            await Task.Yield();
            afterAwait = JobDispatcher.Current;
        }).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Same(dispatcher, beforeAwait);
        Assert.Same(dispatcher, afterAwait);
        Assert.Null(JobDispatcher.Current);
    }

    [Fact]
    public async Task 여러_생산자가_동시에_Post해도_메시지가_유실되지_않는다()
    {
        var dispatcher = new JobDispatcher();
        int value = 0;

        Task<Task[]>[] proceduers = Enumerable.Range(0, 8).Select(_ => Task.Run(() => Enumerable.Range(0, 250)
            .Select(_ => dispatcher.Post(() => value++)).ToArray())).ToArray();

        Task[][] batches = await Task.WhenAll(proceduers);
        Task allMessage = Task.WhenAll(batches.SelectMany(x => x));
        await allMessage.WaitAsync(TimeSpan.FromSeconds(5));

        await Task.WhenAll(proceduers).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(2000, value);
        Assert.Equal(0, dispatcher.PendingCount);

        // 클로저 객체 예시
        // Closure closure = new()
        // {
        //     Values = new(),
        //     Value = 42
        // };

        // 이러면 State에서 values에 대한 참조는 하지만 클로저 객체가 추가 할당이 되는 것을 막을 수 있다
        List<int> values = new();
        await dispatcher.Post((values: values, value: 42),
            static state => state.values.Add(state.value));
    }

    [Fact]
    public async Task State_오버로드는_클로저_캡처없이_실행된다()
    {
        var dispatcher = new JobDispatcher();
        var values = new List<int>();

        await dispatcher.Post(
                (values: values, value: 42),
                static state => state.values.Add(state.value))
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal([42], values);
    }

    [Fact]
    public async Task 비동기_State_오버로드는_완료될때까지_다음_메시지를_막는다()
    {
        var dispatcher = new JobDispatcher();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var values = new List<int>();

        Task first = dispatcher.Post((values, release: release.Task),
            static async state =>
            {
                await state.release;
                state.values.Add(1);
            });

        Task second = dispatcher.Post(
            values,
            static state => state.Add(2));

        Assert.Empty(values);
        release.SetResult();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal([1, 2], values);
    }

    // 1단계: 상태 전환의 기본
    [Fact]
    public void 새_디스패처는_Active_상태다()
    {
        // given
        JobDispatcher dispatcher = new();

        // when & Then
        Assert.Equal(ActorState.Active, dispatcher.State);
    }

    [Fact]
    public async Task 대기중인_메시지가_없을때_닫기를_요청하면_Closed가_된다()
    {
        // given
        JobDispatcher dispatcher = new();

        // when
        dispatcher.RequestClose();

        // then
        Assert.Equal(0, dispatcher.PendingCount);
        await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ActorState.Closed, dispatcher.State);
    }

    [Fact]
    public async Task 메시지를_모두_처리한_후_닫기를_요청하면_Closed가_된다()
    {
        // given
        JobDispatcher dispatcher = new();
        var processed = new ConcurrentQueue<int>();

        // when
        var tasks = Enumerable.Range(0, 100)
            .Select(i => dispatcher.Post(() => processed.Enqueue(i)))
            .ToArray();

        await Task.WhenAll(tasks);

        // then
        Assert.Equal(0, dispatcher.PendingCount);
        Assert.Equal(100, processed.Count);
        Assert.Equal(ActorState.Active, dispatcher.State);

        // 작업을 끝낸 다음에 RequestClose를 호출하면 Closed가 된다.
        dispatcher.RequestClose();
        await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ActorState.Closed, dispatcher.State);
    }

    [Fact]
    public async Task 닫기를_두번_요청해도_예외없이_Closed를_유지한다()
    {
        // given
        JobDispatcher dispatcher = new();

        // when
        dispatcher.RequestClose();
        dispatcher.RequestClose();

        // then
        await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ActorState.Closed, dispatcher.State);
        Assert.Equal(0, dispatcher.PendingCount);
    }

    // 2단계: 새 요청 거절
    [Fact]
    public async Task Closed_상태에서_Post하면_거절된다()
    {
        // given
        JobDispatcher dispatcher = new();

        // when
        dispatcher.RequestClose();

        // then
        await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.Post(() => { }));
    }

    [Fact]
    public async Task Closed_상태에서_Post한_액션은_실행되지_않는다()
    {
        // given
        JobDispatcher dispatcher = new();
        var processed = new ConcurrentQueue<int>();

        // when
        dispatcher.RequestClose();

        // then
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            dispatcher.Post(() => processed.Enqueue(1)));
        Assert.Empty(processed);
        Assert.Equal(ActorState.Closed, dispatcher.State);
    }

    [Fact]
    public async Task Closing_상태에서_Post하면_거절된다()
    {
        // given
        JobDispatcher dispatcher = new();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = dispatcher.Post(async () => await release.Task);
        dispatcher.RequestClose();

        // when & Then
        Assert.Equal(ActorState.Closing, dispatcher.State);
        await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.Post(() => { }));

        release.SetResult();
        await first.WaitAsync(TimeSpan.FromSeconds(5));
    }

    // 3단계: 남은 메시지 마저 처리 (graceful drain)
    [Fact]
    public async Task 실행중에_닫기를_요청하면_끝날때까지_Closing을_유지한다()
    {
        // given
        JobDispatcher dispatcher = new();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = dispatcher.Post(async () => await release.Task);
        dispatcher.RequestClose();

        Assert.Equal(ActorState.Closing, dispatcher.State);

        // when
        release.SetResult();
        await first.WaitAsync(TimeSpan.FromSeconds(5));

        // then
        await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ActorState.Closed, dispatcher.State);
    }

    [Fact]
    public async Task 닫기_요청_전에_들어온_메시지는_모두_순서대로_처리된다()
    {
        // given
        var dispatcher = new JobDispatcher();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processed = new ConcurrentQueue<int>();

        var first = dispatcher.Post(async () =>
        {
            processed.Enqueue(0);
            await release.Task;
        });
        var second = dispatcher.Post(() => processed.Enqueue(1));
        var third = dispatcher.Post(() => processed.Enqueue(2));
        dispatcher.RequestClose();
        Assert.Equal(ActorState.Closing, dispatcher.State);

        // when
        release.SetResult();
        await Task.WhenAll(first, second, third).WaitAsync(TimeSpan.FromSeconds(5));

        // then
        Assert.Equal(Enumerable.Range(0, 3), processed);
        await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ActorState.Closed, dispatcher.State);
    }

    [Fact]
    public async Task 마지막_메시지가_완료된_후에_Closed가_완료된다()
    {
        // given
        var dispatcher = new JobDispatcher();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = dispatcher.Post(async () => { await release.Task; });
        dispatcher.RequestClose();

        Assert.Equal(ActorState.Closing, dispatcher.State);
        Assert.False(dispatcher.Closed.IsCompleted);

        // when
        release.SetResult();

        // then
        await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(first.IsCompleted);
        Assert.Equal(ActorState.Closed, dispatcher.State);
    }

    [Fact]
    public async Task Closing중_처리되는_메시지가_예외를_던져도_Closed에_도달한다()
    {
        // given
        var dispatcher = new JobDispatcher();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processed = new ConcurrentQueue<int>();


        var first = dispatcher.Post(async () =>
        {
            processed.Enqueue(0);
            await release.Task;
            throw new ApplicationException("Boom");
        });
        dispatcher.RequestClose();
        Assert.Equal(ActorState.Closing, dispatcher.State);

        // when
        release.SetResult();

        // then
        ApplicationException exception = await Assert.ThrowsAsync<ApplicationException>(() => first)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("Boom", exception.Message);
        Assert.Equal(Enumerable.Range(0, 1), processed);
        await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(ActorState.Closed, dispatcher.State);
    }

    // 4단계: 동시성 (스트레스 테스트)
    [Fact]
    public async Task Post와_닫기가_동시에_일어나도_대기상태로_남는_Task가_없다()
    {
        for (int round = 0; round < 200; round++)
        {
            // given
            var dispatcher = new JobDispatcher();
            var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var processed = new ConcurrentQueue<int>();
            int late = 0;
            
            Action<int> onExecuted = i =>
            {
                if (dispatcher.State == ActorState.Closed)
                    Interlocked.Increment(ref late);
                processed.Enqueue(i);
            };
            
            // 4개의 이상의 쓰레드가 각각의 작업을 처리할 수 있도록 ProduceUntilRejected 메서드를 병렬로 실행합니다.
            var producers = Enumerable.Range(0, 4)
                .Select(_ => Task.Run(() =>
                    ProduceUntilRejected(dispatcher, startGate.Task, onExecuted)))
                .ToArray();
            var close = Task.Run(async () =>
            {
                await startGate.Task;
                dispatcher.RequestClose();
            });
        
            // when
            startGate.SetResult();
        
            List<Task>[] perProducer = await Task.WhenAll(producers);    // 4명 각자의 접수 목록 4개
            await close;

            // then
            var accepted = perProducer.SelectMany(list => list).ToList();
            await Task.WhenAll(accepted ).WaitAsync(TimeSpan.FromSeconds(5));
            await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(ActorState.Closed, dispatcher.State);
            Assert.Equal(accepted.Count, processed.Count);
            Assert.Equal(0, dispatcher.PendingCount);
            Assert.True(late == 0, $"round {round}: Closed 이후 실행된 메시지 {late}개");
        }

        // 출발 신호를 기다렸다가, 거절될 때까지 Post하고, 수락된 Task 목록을 돌려준다
        async Task<List<Task>> ProduceUntilRejected(JobDispatcher dispatcher, Task start, Action<int> onExecuted)
        {
            await start;

            var accepted = new List<Task>();
            for (int i = 0; i < 10_000; i++)
            {
                try
                {
                    var i1 = i;
                    accepted.Add(dispatcher.Post(() => onExecuted(i1)));
                }
                catch (InvalidOperationException e)
                {
                    return accepted;
                }
            }

            return accepted;
        }
    }

    [Fact]
    public async Task 여러_스레드가_동시에_닫기를_요청해도_Closed는_한번만_완료된다()
    {
        for (int round = 0; round < 200; round++)
        {
            // given
            var dispatcher = new JobDispatcher();
            var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var closes = Enumerable.Range(0, 8)
                .Select(_ => Task.Run(async () =>
                {
                    await startGate.Task;
                    dispatcher.RequestClose();
                })).ToArray();

            // when
            startGate.SetResult();
            await Task.WhenAll(closes);
            
            // then
            await dispatcher.Closed.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(ActorState.Closed, dispatcher.State);
        }
    }
}
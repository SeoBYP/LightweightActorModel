namespace LightweightActors;

public abstract class Message : IMessage
{
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    
    public Task Completion => _completion.Task;

    public abstract ValueTask ExecuteAsync();

    public void SetResult() => _completion.TrySetResult();

    public void SetException(Exception ex) => _completion.TrySetException(ex);
}
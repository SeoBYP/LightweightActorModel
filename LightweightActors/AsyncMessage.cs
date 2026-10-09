namespace LightweightActors;

public class AsyncMessage(Func<Task> action) : Message
{
    public override ValueTask ExecuteAsync()
    {
        return new(action());
    }
}

internal sealed class AsyncMessage<TState>(TState state, Func<TState, Task> action) : Message
{
    public override ValueTask ExecuteAsync()
    {
        return new(action(state));
    }
}
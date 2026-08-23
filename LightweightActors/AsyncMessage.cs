namespace LightweightActors;

public class AsyncMessage(Func<Task> action) : Message
{
    public override ValueTask ExecuteAsync()
    {
        return new(action());
    }
}
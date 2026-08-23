namespace LightweightActors;

internal interface IMessage
{
    Task Completion { get; } // Post 호출자에게 돌려줄 완료 작업

    ValueTask ExecuteAsync(); // 동기/비동기 작업을 하나의 실행 형태로 통일
    
    void SetResult(); // 메세지가 성공했음을 저장
    
    void SetException(Exception ex); // 메세지 처리 중 발생한 예외를 저장
}
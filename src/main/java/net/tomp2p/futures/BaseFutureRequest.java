package net.tomp2p.futures;

public interface BaseFutureRequest extends BaseFuture{

    public void addRequests(FutureResponse futureResponse);
    
    public void releaseEarly();
}

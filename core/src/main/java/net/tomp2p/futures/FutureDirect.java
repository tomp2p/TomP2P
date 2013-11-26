package net.tomp2p.futures;

import java.io.IOException;

import net.tomp2p.message.Buffer;


//TODO: make two generics
public class FutureDirect extends FutureWrapper<FutureResponse> {
    
    private final FutureResponse futureResponse;
    public FutureDirect(FutureResponse futureResponse) {
        this.futureResponse = futureResponse;
        if(futureResponse!=null) {
            waitFor(futureResponse);
        }
    }
    
    public FutureDirect setFailed0(String failed) {
        setFailed(failed);
        return this;
    }
    
    public FutureDirect awaitUninterruptibly0() {
        futureResponse.awaitUninterruptibly();
        return this;
    }
    
    public Buffer getBuffer() {
        synchronized (lock) {
            return futureResponse.getResponse().getBuffer(0);
        }
    }
    
    public Object object() throws ClassNotFoundException, IOException {
        synchronized (lock) {
            return getBuffer().object();
        }
    }

}

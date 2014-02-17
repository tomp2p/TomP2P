package net.tomp2p.futures;

import java.io.IOException;

import net.tomp2p.message.Buffer;


public class FutureDirect extends FutureWrapper<FutureResponse> {
    
    private final FutureResponse futureResponse;
    public FutureDirect(FutureResponse futureResponse) {
        this.futureResponse = futureResponse;
        if(futureResponse!=null) {
            waitFor(futureResponse);
        }
    }
    
    /**
	 * Set failed that returns this class not not null.
	 * 
	 * @param failed
	 *            The failure string
	 * @return this class (never null)
	 */
    public FutureDirect setFailed0(String failed) {
        setFailed(failed);
        return this;
    }
    
    /**
     * Used for testing only.
     * @return this class (never null)
     */
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

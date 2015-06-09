package net.tomp2p.futures;

import io.netty.buffer.Unpooled;

import java.io.IOException;

import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;


public class FutureDirect extends FutureResponse {
	
	final private boolean isRaw;
	private boolean convertToHeapBuffer = true;
	private Object object = null;
	private Buffer buffer = null;
    
    public FutureDirect(Message requestMessage, boolean isRaw) {
		super(requestMessage);
		this.isRaw = isRaw;
		self(this);
	}
    
    @Override
    public FutureDirect response(Message responseMessage) {
    	
    	synchronized (lock) {
            if (!completedAndNotify()) {
                return this;
            }
            if (responseMessage != null) {
                this.responseMessage = responseMessage;
                // if its ok or nok, the communication was successful.
                // Everything else is a failure in communication
                type = futureSuccessEvaluator().evaluate(request(), responseMessage);
                reason = responseMessage.type().toString();
                
                if(convertToHeapBuffer) {
        			if(isRaw) {
        				int len = responseMessage.buffer(0).buffer().readableBytes();
        				byte[] me = new byte[len];
        				responseMessage.buffer(0).buffer().readBytes(me);
        				buffer = new Buffer(Unpooled.wrappedBuffer(me));
        			} else {
        				try {
    						object = responseMessage().buffer(0).object();
    						responseMessage().buffer(0).buffer().release();
    					} catch (ClassNotFoundException e) {
    						type = FutureType.FAILED;
    		                reason = e.toString();
    					} catch (IOException e) {
    						type = FutureType.FAILED;
    		                reason = e.toString();
    					}
        			}
        		}
                
            } else {
                type = FutureType.OK;
                reason = "Nothing to deliver...";
            }
        }
        notifyListeners();
        return this;
    }
    
    @Override
    public FutureDirect failed(String failed) {
    	super.failed(failed);
    	return this;
    }
    
    @Override
    public FutureDirect awaitUninterruptibly() {
    	super.awaitUninterruptibly();
    	return this;
    }
    

	public Buffer buffer() {
        synchronized (lock) {
        	if(buffer == null) {
        		int len = responseMessage().buffer(0).buffer().readableBytes();
				byte[] me = new byte[len];
				responseMessage().buffer(0).buffer().readBytes(me);
				buffer = new Buffer(Unpooled.wrappedBuffer(me));
            	responseMessage().buffer(0).buffer().release();
            }
        	return buffer;
        }
    }
    
    public Object object() throws ClassNotFoundException, IOException {
        synchronized (lock) {
            if(object == null) {
            	object = responseMessage().buffer(0).object();
            	responseMessage().buffer(0).buffer().release();
            }
            return object;
        }
    }
}

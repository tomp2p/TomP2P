package net.tomp2p.relay;

import java.util.TimerTask;

import net.tomp2p.futures.BaseFutureAdapter;

public class SearchRelaysTask extends TimerTask {
    
    private final RelayManager relayManager;
    private final SearchRelaysTask self;

    public SearchRelaysTask(RelayManager relayManager) {
        this.relayManager = relayManager;
        this.self = this;
    }
    
    public void run() {
        System.out.println("searching");
        relayManager.setupRelays().addListener(new BaseFutureAdapter<RelayFuture>() {
            @Override
            public void operationComplete(RelayFuture future) throws Exception {
                if(future.isSuccess()) {
                    self.cancel();
                }
            }
            
        });
    }


}

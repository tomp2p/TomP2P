package net.tomp2p.p2p;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.peers.Maintainable;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceTask implements Runnable {
    
    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceTask.class);
    private static final int MAX_PING = 5;
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private Peer peer;

    private int intervalMillis = 1000;

    private List<Maintainable> maintainables = new ArrayList<Maintainable>();

    private Map<BaseFuture, PeerAddress> runningFutures = new HashMap<BaseFuture, PeerAddress>();

    private boolean shutdown = false;

    private final Object lock = new Object();
    
    private ScheduledFuture<?> scheduledFuture;

    public void init(Peer peer, ScheduledExecutorService timer) {
        this.peer = peer;
        scheduledFuture = timer.scheduleAtFixedRate(this, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        synchronized (lock) {
            //make sure we only have 5 ping in parallel
            if (shutdown || COUNTER.get() > MAX_PING) {
                return;
            }
            for (Maintainable maintainable : maintainables) {
                PeerStatatistic peerStatatistic = maintainable.nextForMaintenance(runningFutures.values());
                if(peerStatatistic == null) {
                    continue;
                }
                BaseFuture future = peer.ping().setPeerAddress(peerStatatistic.getPeerAddress()).start();
                LOG.debug("maintenance ping from {} to {}", peer.getPeerAddress(), peerStatatistic.getPeerAddress());
                
                peer.notifyAutomaticFutures(future);
                runningFutures.put(future, peerStatatistic.getPeerAddress());
                COUNTER.incrementAndGet();
                future.addListener(new BaseFutureAdapter<BaseFuture>() {
                    @Override
                    public void operationComplete(BaseFuture future) throws Exception {
                    	synchronized (lock) {
                            runningFutures.remove(future);
                            COUNTER.decrementAndGet();
                        }
                    }
                });
            }
        }
    }

    public FutureDone<Void> shutdown() {
    	if(scheduledFuture!=null) {
    		scheduledFuture.cancel(false);
    		//TODO: check if synchronized is really necessary here
    	}
        final FutureDone<Void> futureShutdown = new FutureDone<Void>();
        synchronized (lock) {
            shutdown = true;
            final int max = runningFutures.size();
            final AtomicInteger counter = new AtomicInteger(0);
            for (BaseFuture future : runningFutures.keySet()) {
                future.addListener(new BaseFutureAdapter<BaseFuture>() {
                    @Override
                    public void operationComplete(BaseFuture future) throws Exception {
                        if (counter.incrementAndGet() == max) {
                            futureShutdown.setDone();
                        }
                    }
                });
            }
        }
        return futureShutdown;
    }

    public int getIntervalMillis() {
        return intervalMillis;
    }

    public void setIntervalMillis(int intervalMillis) {
        this.intervalMillis = intervalMillis;
    }
    
    public void addMaintainable(Maintainable maintainable) {
        maintainables.add(maintainable);
    }
}

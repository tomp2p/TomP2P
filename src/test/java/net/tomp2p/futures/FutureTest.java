package net.tomp2p.futures;

import net.tomp2p.peers.Number160;


public class FutureTest extends BaseFutureImpl<FutureTest> {
    
    private int counter;
    private Number160 result;
    final private int i;
    final private int start;
    final private int rounds;
    
    public FutureTest(int i, int start, int rounds) {
        this.i = i;
        this.start = start;
        this.rounds = rounds;
        self(this);
    }
    
    public int getI() {
        return i;
    }
    
    public int getRounds() {
        return rounds;
    }
    
    public int getStart() {
        return start;
    }

    public void setDone(Number160 result, int counter) {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            type = FutureType.OK;
            this.result = result;
            this.counter = counter;
        }
        notifyListerenrs();
    }
    
    public int getCounter() {
        synchronized (lock) {
            return counter;
        }
    }
    
    public Number160 getResult() {
        synchronized (lock) {
            return result;
        }
    }
}

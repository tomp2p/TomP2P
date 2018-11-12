package net.tomp2p.futures;

import net.tomp2p.peers.Number256;


public class FutureTest extends BaseFutureImpl<FutureTest> {
    
    private int counter;
    private Number256 result;
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

    public void setDone(Number256 result, int counter) {
        synchronized (lock) {
            if (!completedAndNotify()) {
                return;
            }
            type = FutureType.OK;
            this.result = result;
            this.counter = counter;
        }
        notifyListeners();
    }
    
    public int getCounter() {
        synchronized (lock) {
            return counter;
        }
    }
    
    public Number256 getResult() {
        synchronized (lock) {
            return result;
        }
    }
}

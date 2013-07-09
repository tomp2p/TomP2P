package net.tomp2p.peers;

public class CircularBuffer {
    private int counter = 0;
    private final int bufferSize;
    private final long[] times;

    public CircularBuffer(final int bufferSize, final long defaultTime) {
        if (bufferSize < 1) {
            throw new IllegalArgumentException("Buffer size must be a positive value");
        }
        this.times = new long[bufferSize];
        this.bufferSize = bufferSize;
        for (int i = 0; i < bufferSize; i++) {
            times[i] = defaultTime;
        }
    }

    public void add(long time) {
        synchronized (this) {
            times[counter++ % bufferSize] = time;
        }
    }

    public long mean() {
        long total = 0;
        for (int i = 0; i < bufferSize; i++) {
            synchronized (this) {
                total = +times[i];
            }
        }
        return total / bufferSize;
    }
}

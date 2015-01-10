package net.tomp2p.peers;

/**
 * Class responsible for measuring and storing a Round Trip Time (RTT)
 * This is used to compare peers with similar distance when routing.
 *
 * @author Sebastian Stephan
 */
public class RTT {
    private long startMeasurementTimestamp = -1;
    private long rtt = -1;
    private boolean isUDP = true;
    private boolean isEstimated = false;

    // How much longer is TCP RTT compared to UDP
    // Since TCP connections use 3-way handshake and an
    // ACK to confirm, we assume a total of 4 transmissions
    // for TCP, which is factor 2 longer than an UDP exchange
    // TODO: Confirm with Wireshark
    private static final double TCP_SCALE_DOWN_FACTOR = 3.5;

    // Constructors
    public RTT() {

    }

    /**
     * Create RTT object with some given RTT, and a flag
     * if the time was measured on a UDP exchange.
     *
     * @param rtt       The rtt to be set in milliseconds
     * @param isUDP     True if this was an UDP exchange, set false
     *                  if it was a TCP exchange.
     */
    public RTT(long rtt, boolean isUDP) {
        this.rtt = rtt;
        this.startMeasurementTimestamp = System.currentTimeMillis();
        this.isUDP = isUDP;
    }

    /**
     * Begin counting the time. Can be called multiple times,
     * as long as it has not yet been stopped
     *
     * @return True if time measurement has been started, False otherwise
     */
    public boolean beginTimeMeasurement(boolean isUDP) {
        if (getRtt() == -1) {
            synchronized (this) {
                this.startMeasurementTimestamp = System.currentTimeMillis();
                this.isUDP = isUDP;
                return true;
            }
        }
        return false;
    }

    /**
     * Stop the time measurement and calculate the rtt
     * BeginTimeMeasurement must have been called before
     * Can only be called once
     *
     * @return True if RTT was set, False otherwise
     */
    public boolean stopTimeMeasurement() {
        synchronized (this) {
            if (getRtt() == -1 && getStartMeasurementTimestamp() > 0) {
                rtt = System.currentTimeMillis() - getStartMeasurementTimestamp();
                return true;
            }
        }
        return false;
    }

    public long getStartMeasurementTimestamp() {
        return startMeasurementTimestamp;
    }

    /**
     * Get round-trip-time in milliseconds. If the RTT was calculated
     * by TCP, the value will be scaled down by TCP_SCALE_DOWN_FACTOR
     *
     * @return round-trip-time in milliseconds, -1 if no measurement done.
     */
    public long getRtt() {
        if (isUDP)
            return rtt;
        else
            return (int)(rtt / TCP_SCALE_DOWN_FACTOR);
    }

    public boolean isUDP() {
        return isUDP;
    }

    /**
     * Returns if the RTT measured is an estimate for some other peer
     *
     * @return True if it is an estimate, false otherwise
     */
    public boolean isEstimated() {
        return isEstimated;
    }

    /**
     * Marks this RTT object as an estimate
     *
     * @return The RTT object
     */
    public RTT setEstimated() {
        isEstimated = true;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (isUDP)
            sb.append("(UDP ");
        else
            sb.append(("TCP "));

        sb.append(getRtt());
        sb.append(")");

        return sb.toString();
    }
}

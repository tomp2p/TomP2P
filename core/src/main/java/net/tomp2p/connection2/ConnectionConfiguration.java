package net.tomp2p.connection2;

public interface ConnectionConfiguration {

    /**
     * @return The time that a connection can be idle before its considered not active for short-lived connections
     */
    int idleTCPSeconds();

    /**
     * @return The time that a connection can be idle before its considered not active for short-lived connections
     */
    int idleUDPSeconds();

    /**
     * @return The time a TCP connection is allowed to be established
     */
    int connectionTimeoutTCPMillis();
    
    /**
     * @return Set to true if the communication should be TCP, default is UDP for routing
     */
    boolean isForceTCP();
    
    /**
     * @return Set to true if the communication should be UDP, default is TCP for request
     */
    boolean isForceUDP();

}
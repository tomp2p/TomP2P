package net.tomp2p.connection;

public interface ConnectionConfiguration {

    /**
     * @return The time that a TCP connection can be idle before it is considered not active for short-lived connections.
     */
    int idleTCPSeconds();

    /**
     * @return The time that a UDP connection can be idle before it is considered not active for short-lived connections.
     */
    int idleUDPSeconds();

    /**
     * @return The time a TCP connection is allowed to be established.
     */
    int connectionTimeoutTCPMillis();
    
    /**
     * @return Set to true, if the communication should be TCP. Default is UDP for routing.
     */
    boolean isForceTCP();
    
    /**
     * @return Set to true, if the communication should be UDP. Default is TCP for request.
     */
    boolean isForceUDP();
    
    /**
     * @return The time that a requester waits for a slow peer to answer.
     */
    int slowResponseTimeoutSeconds();

}
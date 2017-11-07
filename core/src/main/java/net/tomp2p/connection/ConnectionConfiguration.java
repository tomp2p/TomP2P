package net.tomp2p.connection;

public interface ConnectionConfiguration {

   
    /**
     * @return The time that a UDP connection can be idle before it is considered not active for short-lived connections.
     */
    int idleUDPMillis();

    /**
     * @return The time that a requester waits for a slow peer to answer.
     */
    int heartBeatSeconds();

}
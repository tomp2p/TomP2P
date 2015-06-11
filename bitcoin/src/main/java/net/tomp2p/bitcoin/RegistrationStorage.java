package net.tomp2p.bitcoin;


import net.tomp2p.p2p.Registration;

/**
 * Stores verified registrations
 *
 * @author Alexander Mülli
 *
 */
public interface RegistrationStorage {
    void start();
    void stop();
    boolean lookup(Registration registration);
    void store(Registration registration);
}

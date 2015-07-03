package net.tomp2p.bitcoin;


import net.tomp2p.p2p.Registration;

/**
 * Interface for Registration Storage
 * Stores and looks up previously verified registrations
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

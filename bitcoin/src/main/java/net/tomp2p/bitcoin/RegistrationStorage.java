package net.tomp2p.bitcoin;


/**
 * Stores verified registrations
 *
 * @author Alexander Mülli
 *
 */
public interface RegistrationStorage {
    boolean lookup(Registration registration);
}

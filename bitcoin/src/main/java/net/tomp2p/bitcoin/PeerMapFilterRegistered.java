package net.tomp2p.bitcoin;

import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * A filter that will ignore peers with invalid registration
 *
 * @author Alexander Mülli
 */
public class PeerMapFilterRegistered implements PeerMapFilter {


    private static final Logger LOG = LoggerFactory.getLogger(PeerMapFilterRegistered.class);

    private RegistrationService registrationService;

    public PeerMapFilterRegistered(RegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    @Override
    public boolean rejectPeerMap(PeerAddress peerAddress, PeerMap peerMap) {
        RegistrationBitcoin registration = new RegistrationBitcoin(peerAddress);
        return registrationService.verify(registration);
    }

    @Override
    public boolean rejectPreRouting(PeerAddress peerAddress, Collection<PeerAddress> all) {
        RegistrationBitcoin registration = new RegistrationBitcoin(peerAddress);
        return registrationService.verify(registration);
    }
}

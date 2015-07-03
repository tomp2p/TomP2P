package net.tomp2p.bitcoin;

import net.tomp2p.message.Message;
import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RPC;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.RegTestParams;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;

/**
 * Unit test for {@link MessageFilterRegistered}
 * @author Alexander Mülli
 *
 */
public class MessageFilterRegisteredTest {

    private static RegistrationServiceMock registrationService;

    @org.junit.BeforeClass
    public static void setUp() throws Exception {
        URL resource = ProcessTest.class.getResource("/bitcoin-regtest");
        File dir = new File(resource.toURI());
        registrationService = new RegistrationServiceMock(null, RegTestParams.get(), dir, "tomp2p-bitcoin");
    }

    @Test
    public void testRejectPreDispatch() throws Exception {
        MessageFilterRegistered filter = new MessageFilterRegistered(registrationService);
        PeerAddress peerAddress = new PeerAddress(Number160.ZERO);
        // put requests message with header extension is not filtered
        Message msg = new Message().sender(peerAddress).hasHeaderExtension(true).command(RPC.Commands.PUT.getNr());
        Assert.assertFalse(filter.rejectPreDispatch(msg).element0());
        // filter msg without header extension
        msg = msg.hasHeaderExtension(false);
        Assert.assertTrue(filter.rejectPreDispatch(msg).element0());
        // don't filter PING requests
        msg = msg.hasHeaderExtension(true).command(RPC.Commands.PING.getNr());
        Assert.assertFalse(filter.rejectPreDispatch(msg).element0());
    }

}

class RegistrationServiceMock extends RegistrationBitcoinService {
    public RegistrationServiceMock(RegistrationStorage storage, NetworkParameters params, File dir, String filename) {
        super(storage, params, dir, filename);
    }

    @Override
    public boolean verify(RegistrationBitcoin registration) {
        return true;
    }

    @Override
    public boolean authenticate(RegistrationBitcoin registration, Message message) {
       return true;
    }
}

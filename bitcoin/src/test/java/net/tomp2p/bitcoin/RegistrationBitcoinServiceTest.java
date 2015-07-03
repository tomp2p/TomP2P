package net.tomp2p.bitcoin;


import net.tomp2p.message.Message;
import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;
import org.bitcoinj.params.TestNet3Params;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Random;

/**
 * Unit test for {@link RegistrationBitcoinService}
 * @author Alexander Mülli
 *
 */
public abstract class RegistrationBitcoinServiceTest {


    protected static RegistrationBitcoinService registrationService;
    protected static List<Registration> validRegistrations;
    private static final Logger LOG = LoggerFactory.getLogger(RegistrationBitcoinServiceTest.class);

    @BeforeClass
    public static void setUp() throws Exception {
        URL resource = RegistrationServiceRegTestTest.class.getResource("/registrations/testnet");
        File dir = new File(resource.toURI());
        RegistrationStorage registrationStorage = new RegistrationStorageFile(dir, "registration.storage");
        registrationService = new RegistrationBitcoinService(registrationStorage, TestNet3Params.get(), new java.io.File("."), "tomP2P-bitcoin-testnet");
//        registrationService = new RegistrationService(registrationStorage, RegTestParams.get(), new java.io.File("."), "tomP2P-bitcoin-regtest");
        registrationService.start();
        //load registrations form file
        validRegistrations = Utils.readRegistrations(dir, "validRegistrations.array");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        registrationService.stop();
        //TODO: delete wallet
    }

    @Test
    public void testVerify() throws Exception {
        for(Registration reg : validRegistrations) {
            RegistrationBitcoin registration = (RegistrationBitcoin) reg;
            Assert.assertTrue(registrationService.verify(registration));
        }
        // validate registration a second time from storage
        for(Registration reg : validRegistrations) {
            RegistrationBitcoin registration = (RegistrationBitcoin) reg;
            System.out.println(registration);
            Assert.assertTrue(registrationService.verify(registration));
        }
        // test invalid registrations
        for(Registration reg : validRegistrations) {
            RegistrationBitcoin registration = (RegistrationBitcoin) reg;
            // set different peerId to make registration invalid
            registration.setPeerId(new Number160(new Random()));
            System.out.println(registration);
            Assert.assertFalse(registrationService.verify(registration));
        }
    }

    @Test
    public void testAuthenticate() throws Exception {
        for(Registration reg : validRegistrations) {
            RegistrationBitcoin registration = (RegistrationBitcoin) reg;
            Message message = Utils.createDummyMessage();
            message = message.publicKeyAndSign(registration.getKeyPair()).setHintSign();
            message.setVerified();
            Assert.assertTrue(registrationService.authenticate(registration, message));
        }
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        for(Registration reg : validRegistrations) {
            RegistrationBitcoin registration = (RegistrationBitcoin) reg;
            Message message = new Message().publicKeyAndSign(gen.generateKeyPair());
            Assert.assertFalse(registrationService.authenticate(registration, message));
        }

    }

    @Test
    public void testGeneratePeerId() throws Exception {

    }
}
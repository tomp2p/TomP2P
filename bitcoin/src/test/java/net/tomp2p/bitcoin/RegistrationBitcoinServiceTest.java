package net.tomp2p.bitcoin;


import net.tomp2p.message.Message;
import net.tomp2p.p2p.Registration;
import net.tomp2p.peers.Number160;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.TestNet3Params;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.List;
import java.util.Random;

/**
 * Unit test for {@link RegistrationBitcoinService}
 * @author Alexander Mülli
 *
 */
public class RegistrationBitcoinServiceTest {

    private static final NetworkParameters params = TestNet3Params.get();
    protected static RegistrationBitcoinService registrationService;
    protected static List<Registration> validRegistrations;
    private static final Logger LOG = LoggerFactory.getLogger(RegistrationBitcoinServiceTest.class);

    @BeforeClass
    public static void setUp() throws Exception {
        URL resource = RegistrationServiceRegTestTest.class.getResource("/registrations/"+params.getId());
        File dir = new File(resource.toURI());
        RegistrationStorage registrationStorage = new RegistrationStorageFile(dir, "registration.storage");
        registrationService = new RegistrationBitcoinService(registrationStorage, params, new java.io.File("."), params.getId()+".tomp2p");
        registrationService.start();
        //load registrations form file
        validRegistrations = Utils.readRegistrations(dir, "validRegistrations.array");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        registrationService.stop();
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
            LOG.debug("registration: {}", registration);
            Assert.assertTrue(registrationService.verify(registration));
        }
        // test invalid registrations
        for(Registration reg : validRegistrations) {
            RegistrationBitcoin registration = (RegistrationBitcoin) reg;
            // set different peerId to make registration invalid
            registration.setPeerId(new Number160(new Random()));
            LOG.debug("registration: {}", registration);
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
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        KeyPair keyPair = gen.generateKeyPair();
        byte[] key = keyPair.getPublic().getEncoded();

        // loading public key from test resources
        URL resource = RegistrationServiceTestNet3Test.class.getResource("/pubKey.key");
        Path path = Paths.get(resource.getPath());
        byte[] encKey = Files.readAllBytes(path);
        System.out.println("Reading public key from " + resource);
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(encKey);
        KeyFactory keyFactory = KeyFactory.getInstance("DSA");
        PublicKey pubKey = keyFactory.generatePublic(pubKeySpec);
        Long nonce = new Long("4032432432");
        Number160 peerId = registrationService.generatePeerId(pubKey, nonce);
        Assert.assertTrue(peerId.equals(new Number160("0xb223e7b712e8880775c237cf0009f5abd06c68bd")));
    }

    @Test
    public void testRegisterPeer() throws Exception {
        //TODO
    }
}
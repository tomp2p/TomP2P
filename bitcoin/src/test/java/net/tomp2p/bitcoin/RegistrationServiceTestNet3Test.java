package net.tomp2p.bitcoin;

import net.tomp2p.peers.Number160;
import org.bitcoinj.params.TestNet3Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Random;

import static org.junit.Assert.*;

public class RegistrationServiceTestNet3Test {
    private static RegistrationService rs;
    private static KeyPair keyPair;
    private static final Logger LOG = LoggerFactory.getLogger(RegistrationServiceTestNet3Test.class);

    @org.junit.BeforeClass
    static public void setUp() throws Exception {
        rs = new RegistrationService(TestNet3Params.get(), new java.io.File("."), "tomP2P-bitcoin");
        KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
        keyPair = gen.generateKeyPair();
    }

    @org.junit.Test
    public void testStart() throws Exception {
        rs.start();
        System.out.println("Wallet balance: " + rs.kit.wallet().getBalance());
        assertTrue(rs.kit.peerGroup().getConnectedPeers().size() >= 1);
    }

    @org.junit.Test
    public void testRegisterNode() throws Exception {
        rs.registerPeer(keyPair);
    }

    @org.junit.Test
    public void testVerify() throws Exception {
        Registration reg = rs.registration.object();
//        As an alternative: values of a previous transaction
//        Registration reg = new Registration(
//                new Number160("0x55f38ce1"),
//                new Sha256Hash("0000000000007d6ef50291d344a070ffa934fa5decfa771df373639ba4d3c068"),
//                new Sha256Hash("2e2e1a9fe348a7a85b26161cf2b839cf429d288796efe8da3515c227e875e2c6")
//        );
        assertTrue(rs.verify(reg));
        assertFalse(rs.verify(new Registration(
                        new Number160(new Random()),
                        reg.getBlockId(),
                        reg.getTransactionId(),
                        reg.getPublicKey())
        ));
    }

    @org.junit.Test
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
//        System.out.println(Arrays.toString(pubKey.getEncoded()));
        Long nonce = new Long("4032432432");
        Number160 peerId = rs.generatePeerId(pubKey, nonce);
        assertTrue(peerId.equals(new Number160("0xb223e7b712e8880775c237cf0009f5abd06c68bd")));
    }

    @org.junit.AfterClass
    static public void tearDown() {
        rs.stop();
    }
}
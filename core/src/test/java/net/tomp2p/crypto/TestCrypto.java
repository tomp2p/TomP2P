package net.tomp2p.crypto;

import net.tomp2p.peers.Number256;
import org.junit.Assert;
import org.junit.Test;
import org.whispersystems.curve25519.Curve25519;
import org.whispersystems.curve25519.Curve25519KeyPair;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Random;

public class TestCrypto {

    @Test
    public void testChaCha20() throws GeneralSecurityException {
        Random r = new Random(42);
        byte[] msg = new byte[1234];
        r.nextBytes(msg);

        byte[] key = new byte[32];
        r.nextBytes(msg);

        ChaCha20 c = new ChaCha20(key, 0);
        byte[] enc = c.encrypt(msg);

        Assert.assertFalse(Arrays.equals(msg, enc));
        //12 is the nonce length
        Assert.assertEquals(1234 + 12, enc.length); //padding + length

        ChaCha20 d = new ChaCha20(key, 0);
        byte[] dec = d.decrypt(enc);
        Assert.assertArrayEquals(msg, dec);


        c = new ChaCha20(key);
        byte[] enc2 = c.encrypt(enc);
        d = new ChaCha20(key);
        dec = d.decrypt(enc2);
        Assert.assertArrayEquals(enc, dec);
    }

    @Test
    public void test25519Serialization() throws GeneralSecurityException {
        Curve25519KeyPair aliceEmphereal = Crypto.cipher.generateKeyPair();
        Curve25519KeyPair bob = Crypto.cipher.generateKeyPair();

        Number256 peerIdBob = new Number256(bob.getPublicKey());

        System.out.println("shared1 key: "+ Arrays.toString(bob.getPublicKey()));
        System.out.println("shared1 key: "+ Arrays.toString(peerIdBob.toByteArray()));

        byte[] sharedKey1 = Crypto.cipher.calculateAgreement(peerIdBob.toByteArray(), aliceEmphereal.getPrivateKey());

        byte[] sharedKey2 = Crypto.cipher.calculateAgreement(aliceEmphereal.getPublicKey(), bob.getPrivateKey());
        System.out.println("shared key: "+ Arrays.toString(sharedKey1));
        System.out.println("shared key: "+ Arrays.toString(sharedKey2));
        Assert.assertArrayEquals(sharedKey1, sharedKey2);

    }

    @Test
    public void test25519Signature() throws GeneralSecurityException {

        Random r = new Random(42);
        byte[] msg = new byte[1234];
        r.nextBytes(msg);

        Curve25519KeyPair alice = Crypto.cipher.generateKeyPair();

        byte[] sig       = Crypto.cipher.calculateSignature(alice.getPrivateKey(), msg);
        byte[] sig_again = Crypto.cipher.calculateSignature(alice.getPrivateKey(), msg);

        Assert.assertFalse(Arrays.equals(sig, sig_again));
        System.out.println("sig1:"+Arrays.toString(sig));
        System.out.println("sig1:"+Arrays.toString(sig_again));

        boolean retVal = Crypto.cipher.verifySignature(alice.getPublicKey(), msg, sig);
        Assert.assertTrue(retVal);

        //check shared key
        Curve25519KeyPair bob = Crypto.cipher.generateKeyPair();
        byte[] shared1 = Crypto.cipher.calculateAgreement(alice.getPublicKey(), bob.getPrivateKey());
        byte[] shared2 = Crypto.cipher.calculateAgreement(bob.getPublicKey(), alice.getPrivateKey());

        Assert.assertArrayEquals(shared1, shared2);

        byte[] sig2 = Crypto.cipher.calculateSignature(alice.getPrivateKey(), msg);

        boolean retVal2 = Crypto.cipher.verifySignature(alice.getPublicKey(), msg, sig);
        Assert.assertTrue(retVal2);
        Assert.assertFalse(Arrays.equals(sig, sig2));

    }
}

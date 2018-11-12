package net.tomp2p.crypto;

import org.whispersystems.curve25519.Curve25519;
import org.whispersystems.curve25519.SecureRandomProvider;

import java.util.Random;

public class Crypto {

    private static class DebugRandom implements SecureRandomProvider {

        final static Random rnd = new Random(42);

        @Override
        public void nextBytes(byte[] output) {
            rnd.nextBytes(output);
        }

        @Override
        public int nextInt(int maxValue) {
            return rnd.nextInt(maxValue);
        }
    }

    public static SecureRandomProvider defaultSecureRandomProvider = null;

    public static void debug() {
        cipher = Curve25519.getInstance(Curve25519.JAVA, new DebugRandom());
    }

    public static Curve25519 cipher = Curve25519.getInstance(Curve25519.JAVA);
}

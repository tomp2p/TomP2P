//Original: https://github.com/google/tink/tree/master/java/src/main/java/com/google/crypto/tink/subtle

package net.tomp2p.crypto;

// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;

/**
 * <p>ChaCha20 and XChaCha20 have two differences: the size of the nonce and the initial state of
 * the block function that produces a key stream block from a key, a nonce, and a counter.
 *
 */
public class ChaCha20  {
    public static final int BLOCK_SIZE_IN_INTS = 16;
    public static final int BLOCK_SIZE_IN_BYTES = BLOCK_SIZE_IN_INTS * 4;
    public static final int KEY_SIZE_IN_INTS = 8;
    public static final int KEY_SIZE_IN_BYTES = KEY_SIZE_IN_INTS * 4;

    private static final int[] SIGMA =
            toIntArray(
                    new byte[] {
                            'e', 'x', 'p', 'a', 'n', 'd', ' ', '3', '2', '-', 'b', 'y', 't', 'e', ' ', 'k'
                    });

    private static final int NONCE_LENGTH = 12;
    private static final SecureRandom RND = new SecureRandom();

    private final int[] key;
    private final int initialCounter;

    /**
     *
     * @param key A 256-bit key
     * @throws InvalidKeyException
     */
    public ChaCha20(final byte[] key) throws InvalidKeyException {
        this(key, 0);
    }

    /**
     *
     * @param key A 256-bit key
     * @param initialCounter A 32-bit initial counter.  This can be set to any number, but will
     *       usually be zero or one.  It makes sense to use one if we use the
     *       zero block for something else, such as generating a one-time
     *       authenticator key as part of an AEAD algorithm.
     * @throws InvalidKeyException
     */
    public ChaCha20(final byte[] key, int initialCounter) throws InvalidKeyException {
        if (key.length != KEY_SIZE_IN_BYTES) {
            throw new InvalidKeyException("The key length in bytes must be 32.");
        }
        this.key = toIntArray(key);
        this.initialCounter = initialCounter;
    }

    /** Returns the initial state from {@code nonce} and {@code counter}. */
    private int[] createInitialState(final int[] nonce, int counter) {
            if (nonce.length != NONCE_LENGTH / 4) {
                throw new IllegalArgumentException(
                        String.format("ChaCha20 uses 96-bit nonces, but got a %d-bit nonce", nonce.length * 32));
            }
            // Set the initial state based on https://tools.ietf.org/html/rfc8439#section-2.3
            int[] state = new int[ChaCha20.BLOCK_SIZE_IN_INTS];
            // The first four words (0-3) are constants: 0x61707865, 0x3320646e, 0x79622d32, 0x6b206574.
            // The next eight words (4-11) are taken from the 256-bit key by reading the bytes in
            // little-endian order, in 4-byte chunks.
            ChaCha20.setSigmaAndKey(state, this.key);
            // Word 12 is a block counter. Since each block is 64-byte, a 32-bit word is enough for 256
            // gigabytes of data. Ref: https://tools.ietf.org/html/rfc8439#section-2.3.
            state[12] = counter;
            // Words 13-15 are a nonce, which must not be repeated for the same key. The 13th word is the
            // first 32 bits of the input nonce taken as a little-endian integer, while the 15th word is the
            // last 32 bits.
            System.arraycopy(nonce, 0, state, 13, nonce.length);
            return state;
    }

    /**
     * The size of the randomly generated nonces.
     *
     * <p>ChaCha20 uses 12-byte nonces, but XChaCha20 use 24-byte nonces.
     */

    public byte[] encrypt(final byte[] plaintext) throws GeneralSecurityException {
        if (plaintext.length > Integer.MAX_VALUE - NONCE_LENGTH) {
            throw new GeneralSecurityException("plaintext too long");
        }
        ByteBuffer ciphertext = ByteBuffer.allocate(NONCE_LENGTH + plaintext.length);
        encrypt(ciphertext, plaintext);
        return ciphertext.array();
    }

    public void encrypt(ByteBuffer output, final byte[] plaintext) throws GeneralSecurityException {
        if (output.remaining() - NONCE_LENGTH < plaintext.length) {
            throw new GeneralSecurityException("Given ByteBuffer output is too small");
        }

        byte[] nonce = new byte[NONCE_LENGTH];
        RND.nextBytes(nonce);
        output.put(nonce);
        process(nonce, output, ByteBuffer.wrap(plaintext));
    }

    public void encrypt(ByteBuffer output, ByteBuffer plaintext) throws GeneralSecurityException {
        if (output.remaining() - NONCE_LENGTH < plaintext.remaining()) {
            throw new GeneralSecurityException("Given ByteBuffer output is too small");
        }

        byte[] nonce = new byte[NONCE_LENGTH];
        RND.nextBytes(nonce);
        output.put(nonce);
        process(nonce, output, plaintext);

    }

    public byte[] decrypt(final byte[] ciphertext) throws GeneralSecurityException {
        return decrypt(ByteBuffer.wrap(ciphertext));
    }

    public byte[] decrypt(ByteBuffer ciphertext) throws GeneralSecurityException {
        if (ciphertext.remaining() < NONCE_LENGTH) {
            throw new GeneralSecurityException("ciphertext too short");
        }
        byte[] nonce = new byte[NONCE_LENGTH];
        ciphertext.get(nonce);
        ByteBuffer plaintext = ByteBuffer.allocate(ciphertext.remaining());
        process(nonce, plaintext, ciphertext);
        return plaintext.array();
    }

    private void process(final byte[] nonce, ByteBuffer output, ByteBuffer input) {
        int length = input.remaining();
        int numBlocks = (length / BLOCK_SIZE_IN_BYTES) + 1;
        for (int i = 0; i < numBlocks; i++) {
            ByteBuffer keyStreamBlock = chacha20Block(nonce, i + initialCounter);
            if (i == numBlocks - 1) {
                // last block
                xor(output, input, keyStreamBlock, length % BLOCK_SIZE_IN_BYTES);
            } else {
                xor(output, input, keyStreamBlock, BLOCK_SIZE_IN_BYTES);
            }
        }
    }

    // https://tools.ietf.org/html/rfc8439#section-2.3.
    private ByteBuffer chacha20Block(final byte[] nonce, int counter) {
        int[] state = createInitialState(toIntArray(nonce), counter);
        int[] workingState = state.clone();
        shuffleState(workingState);
        for (int i = 0; i < state.length; i++) {
            state[i] += workingState[i];
        }
        ByteBuffer out = ByteBuffer.allocate(BLOCK_SIZE_IN_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        out.asIntBuffer().put(state, 0, BLOCK_SIZE_IN_INTS);
        return out;
    }

    private static void setSigmaAndKey(int[] state, final int[] key) {
        System.arraycopy(SIGMA, 0, state, 0, SIGMA.length);
        System.arraycopy(key, 0, state, SIGMA.length, KEY_SIZE_IN_INTS);
    }

    private static void shuffleState(final int[] state) {
        for (int i = 0; i < 10; i++) {
            quarterRound(state, 0, 4, 8, 12);
            quarterRound(state, 1, 5, 9, 13);
            quarterRound(state, 2, 6, 10, 14);
            quarterRound(state, 3, 7, 11, 15);
            quarterRound(state, 0, 5, 10, 15);
            quarterRound(state, 1, 6, 11, 12);
            quarterRound(state, 2, 7, 8, 13);
            quarterRound(state, 3, 4, 9, 14);
        }
    }

    private static void quarterRound(int[] x, int a, int b, int c, int d) {
        x[a] += x[b];
        x[d] = rotateLeft(x[d] ^ x[a], 16);
        x[c] += x[d];
        x[b] = rotateLeft(x[b] ^ x[c], 12);
        x[a] += x[b];
        x[d] = rotateLeft(x[d] ^ x[a], 8);
        x[c] += x[d];
        x[b] = rotateLeft(x[b] ^ x[c], 7);
    }

    private static int[] toIntArray(final byte[] input) {
        IntBuffer intBuffer = ByteBuffer.wrap(input).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
        int[] ret = new int[intBuffer.remaining()];
        intBuffer.get(ret);
        return ret;
    }

    private static int rotateLeft(int x, int y) {
        return (x << y) | (x >>> -y);
    }

    /**
     * Computes the xor of two byte buffers, specifying the length to xor, and
     * stores the result to {@code output}.
     *
     * @return a new byte[] of length len.
     */
    public static final void xor(ByteBuffer output, ByteBuffer x, ByteBuffer y, int len) {
        if (len < 0 || x.remaining() < len || y.remaining() < len || output.remaining() < len) {
            throw new IllegalArgumentException(
                    "That combination of buffers, offsets and length to xor result in out-of-bond accesses.");
        }
        for (int i = 0; i < len; i++) {
            output.put((byte) (x.get() ^ y.get()));
        }
    }
}

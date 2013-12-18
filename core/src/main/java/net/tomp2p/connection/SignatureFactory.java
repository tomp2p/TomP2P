/*
 * Copyright 2013 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.connection;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.security.PublicKey;
import java.security.Signature;

/**
 * This interface is used in the encoder and decoders. A user may set its own signature algorithm.
 * 
 * @author Thomas Bocek
 * 
 */
public interface SignatureFactory {

    /**
     * @return The signature mechanism
     */
    Signature signatureInstance();

    /**
     * The public key is sent over the wire, thus the decoding of it needs special handling.
     * 
     * @param me
     *            The byte array that contains the public key
     * @return The decoded public key
     */
    PublicKey decodePublicKey(byte[] me);

    PublicKey decodePublicKey(ByteBuf buf);

    void encodePublicKey(PublicKey publicKey, CompositeByteBuf buf);

}

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

package net.tomp2p.connection2;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default signature is done with SHA1withDSA.
 * 
 * @author Thomas Bocek
 * 
 */
public class DefaultSignatureFactory implements SignatureFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSignatureFactory.class);

    @Override
    public Signature signatureInstance() {
        try {
            return Signature.getInstance("SHA1withDSA");
        } catch (NoSuchAlgorithmException e) {
            LOG.error("could not find algorithm", e);
            return null;
        }
    }

    @Override
    public PublicKey decodePublicKey(final byte[] me) {
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(me);
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("DSA");
            return keyFactory.generatePublic(pubKeySpec);
        } catch (NoSuchAlgorithmException e) {
            LOG.error("could not find algorithm", e);
            return null;
        } catch (InvalidKeySpecException e) {
            LOG.error("wrong keyspec", e);
            return null;
        }
    }
}

/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.util.crypto;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The password-based encryption algorithm as defined in: 
 * RSA Laboratories, "PKCS #5: Password-Based Encryption Standard," version 1.5
 * @author Martin Zatopek
 *
 */
public class EncryptDESWithMD5 implements EncryptAlgorithm {

    private static Log logger = LogFactory.getLog(EncryptDESWithMD5.class);

    @Override
	public SecretKey createSecretKey(String password) {
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
        SecretKeyFactory keyFac;
        try {
            keyFac = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
            return keyFac.generateSecret(pbeKeySpec);
        } catch (NoSuchAlgorithmException e) {
            logger.error("Can't create secret key with algorithm PBEWithMD5AndDES.");
            return null;
        } catch (InvalidKeySpecException e) {
            logger.error("Can't create secret key with algorithm PBEWithMD5AndDES.");
            return null;
        }
    }

    @Override
	public Object createAlgorithmParameters() {
        //Salt
        byte[] salt = {
            (byte)0xc7, (byte)0x73, (byte)0x21, (byte)0x8c,
            (byte)0x7e, (byte)0xc8, (byte)0xee, (byte)0x99
        };

        //Iteration count
        int count = 20;

        //Create PBE parameter set
        return new PBEParameterSpec(salt, count);
    }

    @Override
	public Cipher createCipher() {
        try {
            return Cipher.getInstance("PBEWithMD5AndDES");
        } catch (NoSuchAlgorithmException e) {
            logger.error("Can't create cipher for algorithm PBEWithMD5AndDES.");
            return null;
        } catch (NoSuchPaddingException e) {
            logger.error("Can't create cipher for algorithm PBEWithMD5AndDES.");
            return null;
        }
    }


}

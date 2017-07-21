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

import java.security.GeneralSecurityException;
import java.security.Key;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * Data encryption class using symmetric cipher to crypt the data and asymmetric cipher to crypt symmetric key.
 * 
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 06/11/10
 */
@SuppressWarnings("EI")
public class CombinedEncryptor {
	// TODO add methods equivalent to Cipher's update&doFinal to support encryption of large data
	private static final String symAlg = "AES";
	private static final int symKeyLen = 128;

	private Cipher symCipher;
	private byte[] hiddenSecret;

	/**
	 * Sole ctor.
	 * @param asymKey Asymmetric to for encryption of generated symmetric key.
	 * @throws GeneralSecurityException
	 */
	public CombinedEncryptor(Key asymKey) throws GeneralSecurityException {
		init(asymKey);
	}
	
	private void init(Key asymKey) throws GeneralSecurityException {
		KeyGenerator kgen = KeyGenerator.getInstance(symAlg);
		kgen.init(symKeyLen);
		SecretKey symKey = kgen.generateKey();
		Cipher asymCipher = Cipher.getInstance(asymKey.getAlgorithm());
		asymCipher.init(Cipher.ENCRYPT_MODE, asymKey);
        hiddenSecret = asymCipher.doFinal(symKey.getEncoded());
        symCipher = Cipher.getInstance(symAlg);
        symCipher.init(Cipher.ENCRYPT_MODE, symKey);
	}

	/**
	 * 
	 * @return Asymmetrically encrypted symmetric key.
	 */
	public byte[] getHiddenSecret() {
		return hiddenSecret;
	}
	
	/**
	 * Continues multi-part encryption.
	 * @param data Data to encrypt.
	 * @return encrypted data.
	 */
	public byte[] update(byte[] data) {
		return symCipher.update(data);
	}

	/**
	 * Continues multi-part encryption.
	 * @param data Data to encrypt.
	 * @param offset Beginning of data to encrypt.
	 * @param len Length of data to encrypt.
	 * @return Encrypted data.
	 */
	public byte[] update(byte[] data, int offset, int len) {
		return symCipher.update(data, offset, len);
	}

	/**
	 * Finishes multi-part encryption
	 * @param data Data to encrypt.
	 * @return
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 */
	public byte[] doFinal(byte[] data) throws IllegalBlockSizeException, BadPaddingException {
		return symCipher.doFinal(data);
	}

	/**
	 * Finishes multi-part encryption
	 * @param data Data to encrypt.
	 * @return
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 */
	public byte[] doFinal(byte[] data, int offset, int len) throws IllegalBlockSizeException, BadPaddingException {
		return symCipher.doFinal(data, offset, len);
	}

}

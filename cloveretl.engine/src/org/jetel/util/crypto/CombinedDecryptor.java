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
import javax.crypto.spec.SecretKeySpec;

/**
 * Data encryption class decrypting data using symmetric key decrypted by an asymmetric key.
 * 
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 06/11/10
 */
public class CombinedDecryptor {
	// TODO add methods equivalent to Cipher's update&doFinal to support encryption of large data
	private static final String symAlg = "AES";

	private Cipher symCipher;

	/**
	 * Sole ctor.
	 * @param asymKey Asymmetric key to be used to decrypt symmetric key.
	 * @param hiddenSecret Asymmetrically encrypted symmetric key.
	 * @throws GeneralSecurityException
	 */
	public CombinedDecryptor(Key asymKey, byte[] hiddenSecret) throws GeneralSecurityException {
		init(asymKey, hiddenSecret);
	}
	
	private void init(Key asymKey, byte[] hiddenSecret) throws GeneralSecurityException {
		Cipher asymCipher = Cipher.getInstance(asymKey.getAlgorithm());
		asymCipher.init(Cipher.DECRYPT_MODE, asymKey);
		SecretKeySpec kspec = new SecretKeySpec(asymCipher.doFinal(hiddenSecret), symAlg);
		symCipher = Cipher.getInstance(symAlg);
		symCipher.init(Cipher.DECRYPT_MODE, kspec);
	}

	/**
	 * Continues multi-part decryption.
	 * @param data Data to decrypt.
	 * @return Decrypted data.
	 */
	public byte[] update(byte[] data) {
		return symCipher.update(data);
	}

	/**
	 * Continues multi-part decryption.
	 * @param data Data to decrypt.
	 * @param offset Beginning of data to decrypt.
	 * @param len Length of data to decrypt.
	 * @return Encrypted data.
	 */
	public byte[] update(byte[] data, int offset, int len) {
		return symCipher.update(data, offset, len);
	}

	/**
	 * Finishes multi-part decryption
	 * @param data Data to decrypt.
	 * @return
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 */
	public byte[] doFinal(byte[] data) throws IllegalBlockSizeException, BadPaddingException {
		return symCipher.doFinal(data);
	}

	/**
	 * Finishes multi-part decryption
	 * @param data Data to decrypt.
	 * @return
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 */
	public byte[] doFinal(byte[] data, int offset, int len) throws IllegalBlockSizeException, BadPaddingException {
		return symCipher.doFinal(data, offset, len);
	}

}

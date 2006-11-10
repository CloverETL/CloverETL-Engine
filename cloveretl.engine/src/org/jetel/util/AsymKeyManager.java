/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * Class for generating, storing and loading keys for asymmetric cryptography
 * 
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 06/11/10
 */
public class AsymKeyManager {
	private static final String alg="RSA";
	private static final int keyLen = 1024;
	private static final int maxKeySize = 4096;

	/**
	 * Create new key pair and save it to filesystem.
	 * @param pubFile File to store public key in.
	 * @param privFile File to store private key in.
	 * @return Generated key pair.
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public static KeyPair generatePair(String pubFile, String privFile) throws IOException, NoSuchAlgorithmException {
		KeyPairGenerator kpgen = KeyPairGenerator.getInstance(alg);
		kpgen.initialize(keyLen);
		KeyPair pair = kpgen.generateKeyPair();
		if (pubFile != null) {
			FileOutputStream outStream = new FileOutputStream(pubFile);
			outStream.write(pair.getPublic().getEncoded());
			outStream.close();
		}
		if (privFile != null) {
			FileOutputStream outStream = new FileOutputStream(privFile);
			outStream.write(pair.getPrivate().getEncoded());
			outStream.close();
		}
		return pair;
	}

	/**
	 * Loads public key from file.
	 * @param pubFile
	 * @return
	 * @throws IOException
	 * @throws InvalidKeySpecException
	 * @throws NoSuchAlgorithmException
	 */
	public static PublicKey readPublic(String pubFile) throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
		try {
			FileInputStream inStream = new FileInputStream(pubFile);
			byte[] buf = new byte[maxKeySize];
			int n = inStream.read(buf);
			byte[] encoded = new byte[n];
			for (int i = 0; i < n; i++) {
				encoded[i] = buf[i];
			}
			X509EncodedKeySpec kspec = new X509EncodedKeySpec(buf);
			return KeyFactory.getInstance(alg).generatePublic(kspec);
		} catch (FileNotFoundException e) {
			return null;
		}
	}

	/**
	 * Loads private key from file.
	 * @param privFile
	 * @return
	 * @throws InvalidKeySpecException
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	public static PrivateKey readPrivate(String privFile) throws InvalidKeySpecException, NoSuchAlgorithmException, IOException {
		try {
			FileInputStream inStream = new FileInputStream(privFile);
			byte[] buf = new byte[maxKeySize];
			int n = inStream.read(buf);
			byte[] encoded = new byte[n];
			for (int i = 0; i < n; i++) {
				encoded[i] = buf[i];
			}
			PKCS8EncodedKeySpec kspec = new PKCS8EncodedKeySpec(buf);
			return KeyFactory.getInstance(alg).generatePrivate(kspec);
		} catch (FileNotFoundException e) {
			return null;
		}
	}

	/* Usage example for AsymKeyManager, CombinedDecriptor, CombinedEncryptor */ 
	public static void main(String args[]) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
		try {
			CombinedEncryptor enc = new CombinedEncryptor(AsymKeyManager.readPrivate("key.priv"));
			CombinedDecryptor dec = new CombinedDecryptor(AsymKeyManager.readPublic("key.pub"), enc.getHiddenSecret());
			byte[] indata = new byte[]{1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0};
			byte[] outdata = dec.doFinal(enc.doFinal(indata));
			outdata = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

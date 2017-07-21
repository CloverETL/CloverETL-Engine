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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;

/**
 * Class for generating, storing and loading keys for asymmetric cryptography
 * 
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 * @since 06/11/10
 */
public class AsymKeyManager {
	private static final String alg="RSA";
	public static final int keyLen = 1024;
	private static final int maxKeySize = 4096;

	/**
	 * Create new key pair and save it to filesystem.
	 * @param pubFile File to store public key in.
	 * @param privFile File to store private key in.
	 * @return Generated key pair.
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public static KeyPair generatePair(OutputStream pubOutput, OutputStream privOutput) throws IOException, NoSuchAlgorithmException {
		KeyPairGenerator kpgen = KeyPairGenerator.getInstance(alg);
		kpgen.initialize(keyLen);
		KeyPair pair = kpgen.generateKeyPair();
		if (pubOutput != null) {
			pubOutput.write(pair.getPublic().getEncoded());
			pubOutput.close();
		}
		if (privOutput != null) {
			privOutput.write(pair.getPrivate().getEncoded());
			privOutput.close();
		}
		return pair;
	}

	/**
	 * Loads public key from a given input stream.
	 * @param pubFile
	 * @return
	 * @throws IOException
	 * @throws InvalidKeySpecException
	 * @throws NoSuchAlgorithmException
	 */
	public static PublicKey readPublic(InputStream inStream) throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {
		byte[] buf = new byte[maxKeySize];
		int n = inStream.read(buf);
		byte[] encoded = new byte[n];
		for (int i = 0; i < n; i++) {
			encoded[i] = buf[i];
		}
		X509EncodedKeySpec kspec = new X509EncodedKeySpec(buf);
		return KeyFactory.getInstance(alg).generatePublic(kspec);
	}

	/**
	 * Loads private key from file.
	 * @param privFile
	 * @return
	 * @throws InvalidKeySpecException
	 * @throws NoSuchAlgorithmException
	 * @throws IOException
	 */
	public static PrivateKey readPrivate(InputStream inStream) throws InvalidKeySpecException, NoSuchAlgorithmException, IOException {
		byte[] buf = new byte[maxKeySize];
		int n = inStream.read(buf);
		byte[] encoded = new byte[n];
		for (int i = 0; i < n; i++) {
			encoded[i] = buf[i];
		}
		PKCS8EncodedKeySpec kspec = new PKCS8EncodedKeySpec(buf);
		return KeyFactory.getInstance(alg).generatePrivate(kspec);
	}

	/* Usage example for AsymKeyManager, CombinedDecriptor, CombinedEncryptor */ 
	public static void main(String args[]) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
		try {
            //generate pair of public and private keys
            AsymKeyManager.generatePair(new FileOutputStream("key.pub"), new FileOutputStream("key.priv"));

            //data definition
            byte[] indata = new byte[]{1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0};
            
            //encryption
			CombinedEncryptor enc = new CombinedEncryptor(AsymKeyManager.readPrivate(new FileInputStream("key.priv")));
            byte[] symetricKey = enc.getHiddenSecret();            
            byte[] encryptedData = enc.doFinal(indata);
            
            //decryption
			CombinedDecryptor dec = new CombinedDecryptor(AsymKeyManager.readPublic(new FileInputStream("key.pub")), symetricKey);
			byte[] outdata = dec.doFinal(encryptedData);

            //print results
            System.out.println(Arrays.toString(outdata));
            System.out.println(enc.getHiddenSecret().length);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

}

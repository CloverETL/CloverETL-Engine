/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
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
 * Created on 7.11.2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */

package org.jetel.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Utility class to encrypt and decrypt passwords. Uses the JCE implementation
 * provided by Sun (SunJCE). JCE and the JCE provider SunJCE are included in
 * J2SE 1.4 and above. For older versions of the Java runtime environment, use
 * <a href="http://java.sun.com/products/jce/index.jsp"> this</a> page. 
 * <p>
 * Before using the encrypt and decrypt functions, one must call the genKey
 * function which will generate the encryption key. The encryption key is kept
 * in the file keystore.dat and the implementation assumes it will be found in
 * the current directory. It is recommended that this file is protected with the
 * proper OS permissions. Regeneration of this file will make it impossible to
 * decrypt previously encrypted passwords.
 * <p>
 * 
 * @author Parth
 */
public class PasswordEncrypt {

	static final int KEYSIZE = 128;

	static final String KEYFILENAME = "keystore.dat";

	static final int ENCRYPT_MODE = Cipher.ENCRYPT_MODE;

	static final int DECRYPT_MODE = Cipher.DECRYPT_MODE;

	static Log logger = LogFactory.getLog(PasswordEncrypt.class);

	SecretKey secretKey = null;

	/**
	 * Generates a key and saves it to keystore.dat. Invocations of encrypt and
	 * decrypt require this file to be found.
	 * 
	 * @return -1 on error, 0 on success
	 */
	public int genKey() {
		int res = 0;
		try {
			KeyGenerator keygen = null;
			keygen = KeyGenerator.getInstance("AES");
			secretKey = keygen.generateKey();
			FileOutputStream fos = new FileOutputStream(KEYFILENAME);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(secretKey);
			oos.flush();
			fos.flush();
			oos.close();
			fos.close();
		} catch (Exception e) {
			res = -1;
			logger.error(e.getMessage());
		}
		return res;
	}

	/*
	 * Read the key from keystore.dat.
	 */
	private int readKey() {
		int res = 0;
		try {
			FileInputStream fis = new FileInputStream(KEYFILENAME);
			ObjectInputStream ois = new ObjectInputStream(fis);
			secretKey = (SecretKey) ois.readObject();
			ois.close();
			fis.close();
		} catch (Exception e) {
			res = -1;
			logger.error(e.getMessage());
		}
		return res;
	}

	/**
	 * Encrypts and returns the input string. The string returned is the base64
	 * encoding of the encrypted string.
	 * 
	 * @param input
	 *            The string to encrypt.
	 * @return The encrypted string.
	 */
	public String encrypt(String input) {
		String output = null;
		if (readKey() == -1) {
			return null;
		}
		byte[] outBytes = doCipher(input.getBytes(), ENCRYPT_MODE);
		byte[] outStrBytes = Base64.encode(outBytes);
		if (outStrBytes != null) {
			output = new String(outStrBytes);
		}
		return output;
	}

	/**
	 * Decrypts and returns the input string. The input string must have been
	 * encrypted using the same key.
	 * 
	 * @param input
	 *            The string to decrypt.
	 * @return The decrypted string.
	 */
	public String decrypt(String input) {
		String output = null;
		if (readKey() == -1) {
			return null;
		}
		byte[] inStrBytes = Base64.decode(input.getBytes());
		byte[] outBytes = doCipher(inStrBytes, DECRYPT_MODE);
		if (outBytes != null) {
			output = new String(outBytes);
		}
		return output;
	}

	/*
	 * Does the actual encoding and decoding. Uses SunJCE as the provider. The
	 * algorithm is AES.
	 */
	private byte[] doCipher(byte[] input, int mode) {
		byte[] output = null;
		try {
			Cipher cipher = Cipher
					.getInstance("AES/ECB/PKCS5Padding", "SunJCE");
			cipher.init(mode, secretKey);
			output = cipher.doFinal(input);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return output;
	}

	/**
	 * <b>Usage:</b>
	 * <p>
	 * To generate a key:
	 * <p>
	 * java org.jetel.util.PasswordEncrypt genkey
	 * <p>
	 * To encrypt a password:
	 * <p>
	 * java org.jetel.util.PasswordEncrypt encrypt <i>password_to_encrypt</i>
	 * <p>
	 * To decrypt a password:
	 * <p>
	 * java org.jetel.util.PasswordEncrypt decrypt <i>password_to_decrypt</i>
	 * <p>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
	    System.out.println("*** Clover.ETL password encrypt utility ***");
		if (args.length == 0 || args[0] == null) {
		    info();
			return;
		}
		if (args[0].equalsIgnoreCase("encrypt")) {
			if (args[1] == null) {
				System.out.println("encrypt requires a password.");
			}
			PasswordEncrypt enc = new PasswordEncrypt();
			System.out.println(enc.encrypt(args[1]));
		} else if (args[0].equalsIgnoreCase("decrypt")) {
			if (args[1] == null) {
				System.out.println("decrypt requires a password.");
				return;
			}
			PasswordEncrypt enc = new PasswordEncrypt();
			System.out.println(enc.decrypt(args[1]));
		} else if (args[0].equalsIgnoreCase("genkey")) {
			PasswordEncrypt enc = new PasswordEncrypt();
			enc.genKey();
			System.out
					.println("A new secret key has been generated and saved into file "
							+ KEYFILENAME );
		} else {
		    info();
			return;
		}
	}
	
	public static void info(){
	     System.out.println("Options:");
	     System.out.println();
		 System.out.println("genkey\t\tto generate a key");
		 System.out.println("encrypt ..password_to_encrypt..\tto encrypt plain text password");
		 System.out.println("decrypt ..password_to_decrypt..\tto decrypt encrypted password");
	}
}

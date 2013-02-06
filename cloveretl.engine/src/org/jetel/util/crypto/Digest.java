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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Digest {

	public enum DigestType {

		MD5("MD5"), SHA("SHA-1"), SHA256("SHA-256");

		private final String type;

		DigestType(String type) {
			this.type = type;
		}

		public String getType() {
			return type;
		}

	}

	public static byte[] digest(DigestType type, byte[] input) {
		MessageDigest algorithm = null;

		try {
			algorithm = MessageDigest.getInstance(type.getType());
		} catch (NoSuchAlgorithmException nsae) {
			throw new RuntimeException(nsae);
		}

		algorithm.reset();
		algorithm.update(input);
		return algorithm.digest();
	}

	public static byte[] digest(DigestType type, String input) {
		return digest(type, input.getBytes());
	}

	/**
	 * Returns MD5 digest string format-compatible with PHP and
	 * command line utilities - for direct str<>str comparison
	 * 
	 * @param md5digest MD5 digest - byte array
	 * @return String representation of MD5 digest
	 */
	public static String MD52String(byte[] md5digest) {
		StringBuilder hexString = new StringBuilder();

		for (int i = 0; i < md5digest.length; i++) {
			String hex = Integer.toHexString(0xFF & md5digest[i]);
			if (hex.length() == 1) {
				hexString.append('0');
			}
			hexString.append(hex);
		}
		return hexString.toString();

	}

}

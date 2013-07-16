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

import java.util.regex.Pattern;

import org.jetel.exception.JetelRuntimeException;

/**
 * Utilities for manipulation with secure graph parameters.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.7.2013
 */
public class SecureParametersUtils {

	private final static Pattern ENCRYPTED_TEXT_PREFIX = Pattern.compile("enc#");

	private final static Pattern ENCRYPTED_TEXT_PATTERN = Pattern.compile(ENCRYPTED_TEXT_PREFIX + "(.*)");

	/**
	 * Checks whether the given text is encrypted text.
	 * Text is encrypted if is prefixed by 'enc#'.
	 * @param text validated text
	 * @return true if input text is encrypted value
	 */
	public static boolean isEncryptedText(String text) {
		return ENCRYPTED_TEXT_PATTERN.matcher(text).matches();
	}
	
	/**
	 * An encrypted text is prefixed by 'enc#' prefix to be possible detected by {@link #isEncryptedText(String)} method.
	 */
	public static String wrapEncryptedText(String encryptedText) {
		return ENCRYPTED_TEXT_PREFIX + encryptedText;
	}

	/**
	 * Removes prefix 'enc#' from given text.
	 */
	public static String unwrapEncryptedText(String encryptedText) {
		if (isEncryptedText(encryptedText)) {
			return ENCRYPTED_TEXT_PATTERN.matcher(encryptedText).group(1);
		} else {
			throw new JetelRuntimeException("Given text '" + encryptedText + "' does not have valid format.");
		}
	}
	
}

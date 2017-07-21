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

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.8.2013
 */
public class SecureParametersUtilsTest extends CloverTestCase {
	
	public void testIsEncryptedText() {
		assertEquals(true, SecureParametersUtils.isEncryptedText("enc#Cw6o+aJi11i8YuFIE+yU3w=="));
		assertEquals(true, SecureParametersUtils.isEncryptedText("enc#"));
		assertEquals(false, SecureParametersUtils.isEncryptedText("enc"));
		assertEquals(false, SecureParametersUtils.isEncryptedText("#"));
		assertEquals(false, SecureParametersUtils.isEncryptedText(""));
		assertEquals(false, SecureParametersUtils.isEncryptedText(null));
	}

	public void testUnwrapEncryptedText() {
		assertEquals("Cw6o+aJi11i8YuFIE+yU3w==", SecureParametersUtils.unwrapEncryptedText("enc#Cw6o+aJi11i8YuFIE+yU3w=="));
		assertEquals("", SecureParametersUtils.unwrapEncryptedText("enc#"));
		
		try {
			assertEquals(false, SecureParametersUtils.unwrapEncryptedText("enc"));
			assertTrue(false);
		} catch (Exception e) {
			//correct
		}
		try {
			assertEquals(false, SecureParametersUtils.unwrapEncryptedText("#"));
			assertTrue(false);
		} catch (Exception e) {
			//correct
		}
		try {
			assertEquals(false, SecureParametersUtils.unwrapEncryptedText(""));
			assertTrue(false);
		} catch (Exception e) {
			//correct
		}
		try {
			assertEquals(false, SecureParametersUtils.unwrapEncryptedText(null));
			assertTrue(false);
		} catch (Exception e) {
			//correct
		}
	}

	public void testWrapEncryptedText() {
		assertEquals("enc#Cw6o+aJi11i8YuFIE+yU3w==", SecureParametersUtils.wrapEncryptedText("Cw6o+aJi11i8YuFIE+yU3w=="));
		assertEquals("enc#enc#Cw6o+aJi11i8YuFIE+yU3w==", SecureParametersUtils.wrapEncryptedText("enc#Cw6o+aJi11i8YuFIE+yU3w=="));
		assertEquals("enc#", SecureParametersUtils.wrapEncryptedText(""));
		assertEquals("enc#", SecureParametersUtils.wrapEncryptedText(null));
	}
	
}

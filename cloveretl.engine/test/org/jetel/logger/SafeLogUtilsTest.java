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
package org.jetel.logger;

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.6.2013
 */
public class SafeLogUtilsTest extends CloverTestCase {

	public void testObfuscateSensitiveInformation() {
		assertEquals(null, SafeLogUtils.obfuscateSensitiveInformation(null));
		assertEquals("", SafeLogUtils.obfuscateSensitiveInformation(""));
		assertEquals("abc", SafeLogUtils.obfuscateSensitiveInformation("abc"));
		assertEquals("abc://defg", SafeLogUtils.obfuscateSensitiveInformation("abc://defg"));
		assertEquals("://de:fg", SafeLogUtils.obfuscateSensitiveInformation("://de:fg"));
		assertEquals("abc://de:***@", SafeLogUtils.obfuscateSensitiveInformation("abc://de:fg@"));
		assertEquals("abc://de@fg", SafeLogUtils.obfuscateSensitiveInformation("abc://de@fg"));
		assertEquals("abc://de:***@fg", SafeLogUtils.obfuscateSensitiveInformation("abc://de:@fg"));
		assertEquals("a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:c@d"));
		assertEquals("://b:c@d", SafeLogUtils.obfuscateSensitiveInformation("://b:c@d"));
		assertEquals("a://b:***@", SafeLogUtils.obfuscateSensitiveInformation("a://b:c@"));
		assertEquals("a://:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://:c@d"));
		assertEquals("a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:@d"));
		assertEquals("a:a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a:a://b:c@d"));
		assertEquals("a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:a b@d"));
		assertEquals("a://b/:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b/:a@d"));
		assertEquals("a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:a@a@d"));
		assertEquals("a://b:***@d a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:c@d a://b:c@d"));
		assertEquals("ftp://test:***@koule", SafeLogUtils.obfuscateSensitiveInformation("ftp://test:test@koule"));
		// CLO-6064: test multiline string
		assertEquals("a\nftp://test:***@koule", SafeLogUtils.obfuscateSensitiveInformation("a\nftp://test:test@koule"));

		assertEquals("ftp://test:***@koule/nonExistingDir/nonExistingFile.txt",
				SafeLogUtils.obfuscateSensitiveInformation("ftp://test:my@test@koule/nonExistingDir/nonExistingFile.txt"));

		assertEquals("asd ftp://user1:***@nonExistingFile.txt;ftp://user2:***@koule/nonExistingDir/nonExistingFile.txt",
				SafeLogUtils.obfuscateSensitiveInformation("asd ftp://user1:bvc@test1@nonExistingFile.txt;ftp://user2:bvc@test2@koule/nonExistingDir/nonExistingFile.txt"));
	}

}

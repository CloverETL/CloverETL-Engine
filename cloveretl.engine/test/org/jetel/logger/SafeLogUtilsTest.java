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
		assertEquals("a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:c@d"));
		assertEquals("://b:c@d", SafeLogUtils.obfuscateSensitiveInformation("://b:c@d"));
		assertEquals("a://b:c@", SafeLogUtils.obfuscateSensitiveInformation("a://b:c@"));
		assertEquals("a://:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://:c@d"));
		assertEquals("a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:@d"));
		assertEquals("a:a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a:a://b:c@d"));
		assertEquals("a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:a b@d"));
		assertEquals("a://b/:a@d", SafeLogUtils.obfuscateSensitiveInformation("a://b/:a@d"));
		assertEquals("a://b:***@a@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:a@a@d"));
		assertEquals("a://b:***@d a://b:***@d", SafeLogUtils.obfuscateSensitiveInformation("a://b:c@d a://b:c@d"));
	}

}

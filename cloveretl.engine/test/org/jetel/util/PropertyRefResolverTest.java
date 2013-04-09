/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2009  David Pavlis <david.pavlis@javlin.eu>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.util;

import java.util.Properties;

import org.jetel.test.CloverTestCase;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;

public class PropertyRefResolverTest extends CloverTestCase {

	private static final String TEST_STRING = "\\n `uppercase(\"message\")`";
	
	private PropertyRefResolver resolver;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		Properties properties = new Properties();
		properties.put("dbDriver", "org.mysql.test");
		properties.put("user", "myself");
		properties.put("password", "xxxyyyzzz");
		properties.put("pwd", "${password}");
		properties.put("ctl", "'${eval}'");
		properties.put("eval", "`'do' + 'ne'`");
		properties.put("recursion", "a${recursion}");
		
		System.setProperty("NAME_WITH_EXT_UNDERLINES", "filename.txt");
		System.setProperty("NAMEWITHEXT", "filename.txt");
		System.setProperty("NAME", "filename");
		System.setProperty("NAME_UNDERLINES", "filename");
		System.setProperty("NAME.WITH.EXT.DOTS", "filename.txt");
		System.setProperty("NAME.DOTS", "filename");
		System.setProperty("NAME_WITH_EXT_SYMBOL", "filename_underline.txt");
		System.setProperty("NAME.WITH.EXT.SYMBOL", "filename_dots.txt");
		
		resolver = new PropertyRefResolver(properties);
	}

	public void testResolve() {
		assertEquals("DB driver is: '{org.mysql.test}' ...",
				resolver.resolveRef("DB driver is: '{${dbDriver}}' ..."));
		assertEquals("myself is user",
				resolver.resolveRef("${user} is user"));
		assertEquals("myself/xxxyyyzzz/xxxyyyzzz is user/password",
				resolver.resolveRef("${user}/${password}/${pwd} is user/password"));
		assertEquals("${user1}/${password1}/xxxyyyzzz is user/password",
				resolver.resolveRef("${user1}/${password1}/${pwd} is user/password", RefResFlag.SPEC_CHARACTERS_OFF));
		
		assertEquals("filename.txt", resolver.resolveRef("${NAME_WITH_EXT_UNDERLINES}"));
		assertEquals("filename.txt", resolver.resolveRef("${NAMEWITHEXT}"));
		assertEquals("filename.txt", resolver.resolveRef("${NAME}.txt"));
		assertEquals("filename.txt", resolver.resolveRef("${NAME_UNDERLINES}.txt"));
		assertEquals("${NAME.WITH.EXT.DOTS}", resolver.resolveRef("${NAME.WITH.EXT.DOTS}"));
		assertEquals("${NAME.DOTS}.txt", resolver.resolveRef("${NAME.DOTS}.txt"));
		
		// Property containing dots should be preferred for backwards compatibility
		assertEquals("filename_dots.txt", resolver.resolveRef("${NAME_WITH_EXT_SYMBOL}"));
	}

	public void testEvaluate() {
		assertEquals("<null>", resolver.resolveRef("`null`"));
		assertEquals("1 + 1 = 2", resolver.resolveRef("1 + 1 = `1 + 1`"));
		assertEquals("Hello, World!", resolver.resolveRef("`'H' + 'e' + 'll' + 'o'`, World!"));
		assertEquals("CTL expression: done", resolver.resolveRef("CTL expression: `${ctl}`"));
		assertEquals("m", resolver.resolveRef("`substring('${user}', 0, 1)`"));
		assertEquals("back quoted `CTL expression`", resolver.resolveRef("back quoted `'\\`CTL expression\\`'`"));
		assertEquals("just back quotes (`)", resolver.resolveRef("just back quotes (\\`)"));
		assertEquals("empty CTL expression", resolver.resolveRef("emp``ty CTL expression"));
		
		assertEquals("xxxyyyzzz", resolver.resolveRef("`'$' + '{' + 'pwd' + '}'`"));
		assertEquals("'done'", resolver.resolveRef("`'$' + '{' + 'ctl' + '}'`"));
		
		assertEquals("\n MESSAGE", resolver.resolveRef(TEST_STRING, RefResFlag.REGULAR));
		assertEquals("\n `uppercase(\"message\")`", resolver.resolveRef(TEST_STRING, RefResFlag.CTL_EXPRESSIONS_OFF));
		assertEquals("\\n MESSAGE", resolver.resolveRef(TEST_STRING, RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("\\n `uppercase(\"message\")`", resolver.resolveRef(TEST_STRING, RefResFlag.ALL_OFF));
	}
	
	public void testUnlimitedRecursion() {
		assertTrue(resolver.resolveRef("${recursion}").matches("a*\\$\\{recursion\\}"));
	}

}

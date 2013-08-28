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

import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.GraphParameters;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.PrimitiveAuthorityProxy;
import org.jetel.test.CloverTestCase;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;

public class PropertyRefResolverTest extends CloverTestCase {

	private static final String TEST_STRING = "\\n `uppercase(\"message\")`";
	
	private PropertyRefResolver resolver;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		GraphParameters graphParameters = new GraphParameters();
		graphParameters.addGraphParameter("dbDriver", "org.mysql.test");
		graphParameters.addGraphParameter("user", "myself");
		graphParameters.addGraphParameter("password", "xxxyyyzzz");
		graphParameters.addGraphParameter("pwd", "${password}");
		graphParameters.addGraphParameter("ctl", "'${eval}'");
		graphParameters.addGraphParameter("eval", "`'do' + 'ne'`");
		graphParameters.addGraphParameter("recursion", "a${recursion}");
		
		GraphParameter gp = new GraphParameter("secureParameter", "abc");
		gp.setSecure(true);
		graphParameters.addGraphParameter(gp);
		
		
		System.setProperty("NAME_WITH_EXT_UNDERLINES", "filename.txt");
		System.setProperty("NAMEWITHEXT", "filename.txt");
		System.setProperty("NAME", "filename");
		System.setProperty("NAME_UNDERLINES", "filename");
		System.setProperty("NAME.WITH.EXT.DOTS", "filename.txt");
		System.setProperty("NAME.DOTS", "filename");
		System.setProperty("NAME_WITH_EXT_SYMBOL", "filename_underline.txt");
		System.setProperty("NAME.WITH.EXT.SYMBOL", "filename_dots.txt");
		
		resolver = new PropertyRefResolver(graphParameters);
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
		try {
			resolver.resolveRef("${recursion}");
			assertTrue(false);
		} catch (JetelRuntimeException e) {
			//correct
		}
	}

	public void testSecureParameters() {
		TransformationGraph graph = new TransformationGraph();
		graph.getRuntimeContext().setAuthorityProxy(new PrimitiveAuthorityProxy() {
			@Override
			public String getSecureParamater(String parameterName, String parameterValue) {
				if (parameterName.equals("secureParameter")) {
					return "secureValue";
				} else if (parameterName.equals("password")) {
					return "otherPass";
				} else {
					return null;
				}
			}
		});
		
		Context c = ContextProvider.registerGraph(graph);
		try {
//			assertEquals("neco ${secureParameter} neco", resolver.resolveRef("neco ${secureParameter} neco", RefResFlag.REGULAR));
//			assertEquals("neco ${secureParameter} neco", resolver.resolveRef("neco ${secureParameter} neco", RefResFlag.ALL_OFF));
//			assertEquals("neco ${secureParameter} neco", resolver.resolveRef("neco ${secureParameter} neco", RefResFlag.CTL_EXPRESSIONS_OFF));
//			assertEquals("neco ${secureParameter} neco", resolver.resolveRef("neco ${secureParameter} neco", RefResFlag.SPEC_CHARACTERS_OFF));
			assertEquals("neco secureValue neco", resolver.resolveRef("neco ${secureParameter} neco", RefResFlag.SECURE_PARAMATERS));
			assertEquals("neco secureValue neco myself neco", resolver.resolveRef("neco ${secureParameter} neco ${user} neco", RefResFlag.SECURE_PARAMATERS));
			assertEquals("\\nneco secureValue neco myself neco", resolver.resolveRef("\\nneco ${secureParameter} neco ${user} neco", RefResFlag.SECURE_PARAMATERS));
			assertEquals("\\nneco secureValue neco myself neco", resolver.resolveRef("\\nneco ${secureParameter} neco ${user} neco", RefResFlag.ALL_OFF.resolveSecureParameters(true)));
			assertEquals("\nneco secureValue neco myself neco", resolver.resolveRef("\\nneco ${secureParameter} neco ${user} neco", RefResFlag.ALL_OFF.resolveSecureParameters(true).resolveSpecCharacters(true)));
//			assertEquals("\nneco ${secureParameter} neco myself neco", resolver.resolveRef("\\nneco ${secureParameter} neco ${user} neco", RefResFlag.ALL_OFF.resolveSecureParameters(false).resolveSpecCharacters(true)));
			//assertEquals("otherPass", resolver.resolveRef("${password}", RefResFlag.ALL_OFF.resolveSecureParameters(true)));
			assertEquals("xxxyyyzzz", resolver.resolveRef("${password}", RefResFlag.ALL_OFF.resolveSecureParameters(false)));
		} finally {
			ContextProvider.unregister(c);
		}
	}
	
}

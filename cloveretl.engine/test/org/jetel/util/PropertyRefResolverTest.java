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

import org.jetel.exception.HttpContextNotAvailableException;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.GraphParameters;
import org.jetel.graph.runtime.HttpContext;
import org.jetel.graph.runtime.PrimitiveAuthorityProxy;
import org.jetel.test.CloverTestCase;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.PropertyRefResolver.RecursionOverflowedException;
import org.jetel.util.property.RefResFlag;

public class PropertyRefResolverTest extends CloverTestCase {

	private static final String TEST_STRING = "\\n `uppercase(\"message\")`";
	
	private PropertyRefResolver resolver;

	private HttpContext httpContextMock = new HttpContextMock();
	
	private PrimitiveAuthorityProxyMock authorityProxy;
	
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
		graphParameters.addGraphParameter("specChars", "a\\nb");
		
		graphParameters.addGraphParameter("composition1", "${composition1${composition1Idx}}");
		graphParameters.addGraphParameter("composition1Idx", "A");
		graphParameters.addGraphParameter("composition1A", "Composition1Result");
		graphParameters.addGraphParameter("composition2", "${user}_${${composition2A}}_${pwd}");
		graphParameters.addGraphParameter("composition2A", "nonResolvable2");
		
		GraphParameter gp1 = new GraphParameter("secureParameter", "abc");
		gp1.setSecure(true);
		graphParameters.addGraphParameter(gp1);

		GraphParameter gp2 = new GraphParameter("nonResolvable1", "${pwd}");
		gp2.setCanBeResolved(false);
		graphParameters.addGraphParameter(gp2);

		GraphParameter gp3 = new GraphParameter("nonResolvable2", "abc${user}def${pwd}ghi");
		gp3.setCanBeResolved(false);
		graphParameters.addGraphParameter(gp3);

		GraphParameter gp4 = new GraphParameter("nonResolvable3", "");
		gp4.setCanBeResolved(false);
		graphParameters.addGraphParameter(gp4);

		GraphParameter gp5 = new GraphParameter("nonResolvable4", "${nonResolvable4}");
		gp5.setCanBeResolved(false);
		graphParameters.addGraphParameter(gp5);

		
		System.setProperty("NAME_WITH_EXT_UNDERLINES", "filename.txt");
		System.setProperty("NAMEWITHEXT", "filename.txt");
		System.setProperty("NAME", "filename");
		System.setProperty("NAME_UNDERLINES", "filename");
		System.setProperty("NAME.WITH.EXT.DOTS", "filename.txt");
		System.setProperty("NAME.DOTS", "filename");
		System.setProperty("NAME_WITH_EXT_SYMBOL", "filename_underline.txt");
		System.setProperty("NAME.WITH.EXT.SYMBOL", "filename_dots.txt");
		
		resolver = new PropertyRefResolver(graphParameters);
				
		authorityProxy = new PrimitiveAuthorityProxyMock();
		resolver.setAuthorityProxy(authorityProxy);
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
		
		assertEquals("${pwd}", resolver.resolveRef("${nonResolvable1}"));
		assertEquals("abc${user}def${pwd}ghi", resolver.resolveRef("${nonResolvable2}"));
		assertEquals("ab", resolver.resolveRef("a${nonResolvable3}b"));
		assertEquals("a sda ${pwd} sdabc${user}def${pwd}ghidsd", resolver.resolveRef("a sda ${nonResolvable1} sd${nonResolvable2}dsd"));
		assertEquals("${pwd}abc${user}def${pwd}ghi", resolver.resolveRef("${nonResolvable1}${nonResolvable2}"));
		assertEquals("${pwd}myself", resolver.resolveRef("${nonResolvable1}${user}"));
		assertEquals("myself${pwd}", resolver.resolveRef("${user}${nonResolvable1}"));
		assertEquals("${nonResolvable4}", resolver.resolveRef("${nonResolvable4}"));

		assertEquals("Composition1Result_myself", resolver.resolveRef("${composition1}_${user}"));
		assertEquals("myself_myself_abc${user}def${pwd}ghi_xxxyyyzzz_xxxyyyzzz", resolver.resolveRef("${user}_${composition2}_${pwd}"));
		
		//test REQUEST parameters
		authorityProxy.setHttpContextAvailable(true);
		assertEquals("123", resolver.resolveRef("${REQUEST.a}"));
		assertEquals("123", resolver.resolveRef("${request.a}"));
	}

	public void testEvaluate() {
		assertEquals("`null`", resolver.resolveRef("`null`"));
		assertEquals("1 + 1 = `1 + 1`", resolver.resolveRef("1 + 1 = `1 + 1`"));
		assertEquals("`'H' + 'e' + 'll' + 'o'`, World!", resolver.resolveRef("`'H' + 'e' + 'll' + 'o'`, World!"));
		assertEquals("CTL expression: `'`'do' + 'ne'`'`", resolver.resolveRef("CTL expression: `${ctl}`"));
		assertEquals("`substring('myself', 0, 1)`", resolver.resolveRef("`substring('${user}', 0, 1)`"));
		assertEquals("back quoted `'`CTL expression`'`", resolver.resolveRef("back quoted `'\\`CTL expression\\`'`"));
		assertEquals("just back quotes (`)", resolver.resolveRef("just back quotes (\\`)"));
		assertEquals("emp``ty CTL expression", resolver.resolveRef("emp``ty CTL expression"));
		
		assertEquals("`'$' + '{' + 'pwd' + '}'`", resolver.resolveRef("`'$' + '{' + 'pwd' + '}'`"));
		assertEquals("`'$' + '{' + 'ctl' + '}'`", resolver.resolveRef("`'$' + '{' + 'ctl' + '}'`"));
		
		assertEquals("\n `uppercase(\"message\")`", resolver.resolveRef(TEST_STRING, RefResFlag.REGULAR));
		assertEquals("\\n `uppercase(\"message\")`", resolver.resolveRef(TEST_STRING, RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("\\n `uppercase(\"message\")`", resolver.resolveRef(TEST_STRING, RefResFlag.ALL_OFF));
	}
	
	public void testUnlimitedRecursion() {
		try {
			resolver.resolveRef("${recursion}");
			assertTrue(false);
		} catch (RecursionOverflowedException e) {
			//correct
		}
	}

	public void testSecureParameters() {
		assertEquals("neco secureValue neco", resolver.resolveRef("neco ${secureParameter} neco", RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("neco secureValue neco myself neco", resolver.resolveRef("neco ${secureParameter} neco ${user} neco", RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("\\nneco secureValue neco myself neco", resolver.resolveRef("\\nneco ${secureParameter} neco ${user} neco", RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("\\nneco secureValue neco myself neco", resolver.resolveRef("\\nneco ${secureParameter} neco ${user} neco", RefResFlag.ALL_OFF));
		assertEquals("\nneco secureValue neco myself neco", resolver.resolveRef("\\nneco ${secureParameter} neco ${user} neco", RefResFlag.ALL_OFF.resolveSpecCharacters(true)));
		assertEquals("xxxyyyzzz", resolver.resolveRef("${password}", RefResFlag.ALL_OFF));
	}
	
	public void testIsPropertyReference() {
		assertFalse(PropertyRefResolver.isPropertyReference(null));
		assertFalse(PropertyRefResolver.isPropertyReference(""));
		assertFalse(PropertyRefResolver.isPropertyReference("a"));
		assertFalse(PropertyRefResolver.isPropertyReference("$"));
		assertFalse(PropertyRefResolver.isPropertyReference("${}"));
		assertFalse(PropertyRefResolver.isPropertyReference("${a"));
		assertFalse(PropertyRefResolver.isPropertyReference("$a}"));
		assertFalse(PropertyRefResolver.isPropertyReference("$a"));
		assertTrue(PropertyRefResolver.isPropertyReference("${a}"));
		assertTrue(PropertyRefResolver.isPropertyReference("${abc}"));
		assertFalse(PropertyRefResolver.isPropertyReference("b${a}"));
		assertFalse(PropertyRefResolver.isPropertyReference("${{}"));
		assertTrue(PropertyRefResolver.isPropertyReference("${a_b}"));
		assertFalse(PropertyRefResolver.isPropertyReference("${aa.cc}"));
		assertFalse(PropertyRefResolver.isPropertyReference("${aa.cc}asd"));
		assertFalse(PropertyRefResolver.isPropertyReference("${123}"));
		assertFalse(PropertyRefResolver.isPropertyReference("${123abc}"));
		assertTrue(PropertyRefResolver.isPropertyReference("${abc123}"));
		assertFalse(PropertyRefResolver.isPropertyReference("${a}${b}"));
		
		authorityProxy.setHttpContextAvailable(true);
		assertTrue(PropertyRefResolver.isPropertyReference("${request.a}"));
		assertTrue(PropertyRefResolver.isPropertyReference("${REQUEST.a}"));
		assertFalse(PropertyRefResolver.isPropertyReference("${.a}"));
		
	}

	public void testContainsProperty() {
		assertFalse(PropertyRefResolver.containsProperty(null));
		assertFalse(PropertyRefResolver.containsProperty(""));
		assertFalse(PropertyRefResolver.containsProperty("a"));
		assertFalse(PropertyRefResolver.containsProperty("$"));
		assertFalse(PropertyRefResolver.containsProperty("${}"));
		assertFalse(PropertyRefResolver.containsProperty("${a"));
		assertFalse(PropertyRefResolver.containsProperty("$a}"));
		assertFalse(PropertyRefResolver.containsProperty("$a"));
		assertTrue(PropertyRefResolver.containsProperty("${a}"));
		assertTrue(PropertyRefResolver.containsProperty("${abc}"));
		assertTrue(PropertyRefResolver.containsProperty("b${a}"));
		assertFalse(PropertyRefResolver.containsProperty("${{}"));
		assertTrue(PropertyRefResolver.containsProperty("${a_b}"));
		assertFalse(PropertyRefResolver.containsProperty("${aa.cc}"));
		assertFalse(PropertyRefResolver.containsProperty("${aa.cc}asd"));
		assertFalse(PropertyRefResolver.containsProperty("${123}"));
		assertFalse(PropertyRefResolver.containsProperty("${123abc}"));
		assertTrue(PropertyRefResolver.containsProperty("${abc123}"));

		assertTrue(PropertyRefResolver.containsProperty("a${b}c"));
		assertTrue(PropertyRefResolver.containsProperty("${b}c"));
		assertTrue(PropertyRefResolver.containsProperty("a${b}"));
		assertTrue(PropertyRefResolver.containsProperty("a${b}c${d}e"));
		assertTrue(PropertyRefResolver.containsProperty("a${b}c${123}e"));
		assertFalse(PropertyRefResolver.containsProperty("a${123}c${a.b}e"));
	}

	public void testGetResolvedPropertyValue() {
		assertEquals("org.mysql.test", resolver.getResolvedPropertyValue("dbDriver", null));
		assertEquals("secureValue", resolver.getResolvedPropertyValue("secureParameter", RefResFlag.REGULAR));
		assertEquals("xxxyyyzzz", resolver.getResolvedPropertyValue("pwd", RefResFlag.REGULAR));
		assertEquals("a\nb", resolver.getResolvedPropertyValue("specChars", null));
		assertEquals("a\nb", resolver.getResolvedPropertyValue("specChars", RefResFlag.REGULAR));
		assertEquals("a\\nb", resolver.getResolvedPropertyValue("specChars", RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals(null, resolver.getResolvedPropertyValue("nonexisting_parameter", RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("${nonResolvable4}", resolver.getResolvedPropertyValue("nonResolvable4", RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("abc${user}def${pwd}ghi", resolver.getResolvedPropertyValue("nonResolvable2", RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("Composition1Result", resolver.getResolvedPropertyValue("composition1", RefResFlag.SPEC_CHARACTERS_OFF));
		assertEquals("myself_abc${user}def${pwd}ghi_xxxyyyzzz", resolver.getResolvedPropertyValue("composition2", RefResFlag.SPEC_CHARACTERS_OFF));
		
		try {
			resolver.getResolvedPropertyValue("", RefResFlag.REGULAR);
			assertTrue(false);
		} catch (IllegalArgumentException e) {
			//CORRECT
		}
		try {
			resolver.getResolvedPropertyValue(null, RefResFlag.REGULAR);
			assertTrue(false);
		} catch (IllegalArgumentException e) {
			//CORRECT
		}
	}
	
	public void testGetReferencedProperty() {
		assertEquals(null, PropertyRefResolver.getReferencedProperty(null));
		assertEquals(null, PropertyRefResolver.getReferencedProperty(""));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("a"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("$"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${a"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("$a}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("$a"));
		assertEquals("a", PropertyRefResolver.getReferencedProperty("${a}"));
		assertEquals("abc", PropertyRefResolver.getReferencedProperty("${abc}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("b${a}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${{}"));
		assertEquals("a_b", PropertyRefResolver.getReferencedProperty("${a_b}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${aa.cc}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${aa.cc}asd"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${123}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${123abc}"));
		assertEquals("abc123", PropertyRefResolver.getReferencedProperty("${abc123}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${a}${b}"));
		
		authorityProxy.setHttpContextAvailable(true);
		assertEquals("REQUEST.a", PropertyRefResolver.getReferencedProperty("${REQUEST.a}"));
		assertEquals("request.a", PropertyRefResolver.getReferencedProperty("${request.a}"));
		assertEquals(null, PropertyRefResolver.getReferencedProperty("${.a}"));
	}
	
	private class PrimitiveAuthorityProxyMock extends PrimitiveAuthorityProxy {
		
		boolean httpContextAvailable;
		
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
		
		@Override
		public HttpContext getHttpContext() throws HttpContextNotAvailableException {
			return httpContextMock;
		}
		
		@Override
		public boolean isHttpContextAvailable() {
			return httpContextAvailable;
		}
		
		public void setHttpContextAvailable(boolean available) {
			httpContextAvailable = available;
		}
	};
}

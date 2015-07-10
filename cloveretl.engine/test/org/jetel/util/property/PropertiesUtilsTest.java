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
package org.jetel.util.property;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.6.2012
 */
public class PropertiesUtilsTest extends CloverTestCase {

	private static final String LINE_SEPARATOR = System.getProperties().getProperty("line.separator");
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	public void testParseFormatProperties() {
		String s;
		Properties properties = new Properties();
		
		assertNull(s = PropertiesUtils.formatProperties(null));
		assertNull(PropertiesUtils.parseProperties(s));

		assertEquals("", s = PropertiesUtils.formatProperties(properties));
		assertEquals(properties, PropertiesUtils.parseProperties(s));

		properties.setProperty("martin", "kokon");
		assertEquals("martin=kokon" + LINE_SEPARATOR, s = PropertiesUtils.formatProperties(properties));
		assertEquals(properties, PropertiesUtils.parseProperties(s));
		
		properties.setProperty("key1", "");
		s = PropertiesUtils.formatProperties(properties);
		assertEquals(properties, PropertiesUtils.parseProperties(s));

		properties.setProperty("", "neco");
		s = PropertiesUtils.formatProperties(properties);
		assertEquals(properties, PropertiesUtils.parseProperties(s));

		properties.setProperty("", "");
		s = PropertiesUtils.formatProperties(properties);
		assertEquals(properties, PropertiesUtils.parseProperties(s));
	}
	
	public void testCopy() {
		Properties empty = new Properties();
		Properties empty1 = new Properties();

		PropertiesUtils.copy(null, null);
		
		PropertiesUtils.copy(null, empty);
		assertTrue(empty.size() == 0);
		
		PropertiesUtils.copy(empty, empty1);
		assertTrue(empty.size() == 0);
		assertTrue(empty1.size() == 0);

		Properties prop = new Properties();
		prop.setProperty("neco", "neco1");
		PropertiesUtils.copy(empty, prop);
		assertTrue(empty.size() == 0);
		assertTrue(prop.size() == 1);
		assertEquals("neco1", prop.getProperty("neco"));
		
		PropertiesUtils.copy(prop, prop);
		assertTrue(prop.size() == 1);
		assertEquals("neco1", prop.getProperty("neco"));
		
		Properties prop1 = new Properties();
		PropertiesUtils.copy(prop, prop1);
		assertTrue(prop.size() == 1);
		assertEquals("neco1", prop.getProperty("neco"));
		assertTrue(prop1.size() == 1);
		assertEquals("neco1", prop1.getProperty("neco"));
		
		prop.setProperty("dalsi", "neco2");
		prop.setProperty("dalsi2", "");
		prop1.setProperty("xxx", "yyy");
		PropertiesUtils.copy(prop, prop1);
		assertTrue(prop.size() == 3);
		assertEquals("neco1", prop.getProperty("neco"));
		assertEquals("neco2", prop.getProperty("dalsi"));
		assertEquals("", prop.getProperty("dalsi2"));
		assertEquals(null, prop.getProperty("dalsi3"));
		assertTrue(prop1.size() == 4);
		assertEquals("neco1", prop1.getProperty("neco"));
		assertEquals("yyy", prop1.getProperty("xxx"));
		assertEquals("neco2", prop1.getProperty("dalsi"));
		assertEquals("", prop1.getProperty("dalsi2"));
		assertEquals(null, prop1.getProperty("dalsi3"));
	}
	
	public void testDuplicate() {
		assertEquals(null, PropertiesUtils.duplicate(null));
		
		Properties prop = PropertiesUtils.duplicate(new Properties());
		assertTrue(prop.size() == 0);

		prop = PropertiesUtils.duplicate(prop);
		assertTrue(prop.size() == 0);

		prop.setProperty("xxx", "yyy");
		Properties prop1 = PropertiesUtils.duplicate(prop);
		assertTrue(prop.size() == 1);
		assertEquals("yyy", prop.getProperty("xxx"));
		assertTrue(prop1.size() == 1);
		assertEquals("yyy", prop1.getProperty("xxx"));

		prop.setProperty("xxx", "yyy");
		prop.setProperty("neco", "");
		prop1 = PropertiesUtils.duplicate(prop);
		assertTrue(prop.size() == 2);
		assertEquals("yyy", prop.getProperty("xxx"));
		assertEquals("", prop.getProperty("neco"));
		assertTrue(prop1.size() == 2);
		assertEquals("yyy", prop1.getProperty("xxx"));
		assertEquals("", prop1.getProperty("neco"));
	}
	
	private static <K, V> Set<Entry<K, V>> difference(Map<K, V> map1, Map<K, V> map2) {
	    Set<Entry<K, V>> diff = new LinkedHashSet<Entry<K, V>>((map1.entrySet()));
	    diff.addAll(map2.entrySet());//Union
	    Set<Entry<K, V>> tmp = new HashSet<Entry<K, V>>((map1.entrySet()));
	    tmp.retainAll(map2.entrySet());//Intersection
	    diff.removeAll(tmp);//Diff

	    return diff;
	}
	
	@SuppressWarnings("unchecked")
	private static String diff(Map<?, ?> map1, Map<?, ?> map2) {
	    return difference((Map<Object, Object>) map1, (Map<Object, Object>) map2).toString();
	}
	
	public void testDeserialize() throws Exception {
		assertEquals(Collections.emptyMap(), PropertiesUtils.deserialize(null));
		assertEquals(Collections.emptyMap(), PropertiesUtils.deserialize(""));
		assertEquals(Collections.emptyMap(), PropertiesUtils.deserialize("   "));
		assertEquals(Collections.emptyMap(), PropertiesUtils.deserialize("   \n   "));
		
		Map<String, String> expected;
		
		expected = new HashMap<>();
		expected.put("xxx", "1");
		expected.put("kkk", "2");
		expected.put("include", "file.properties");
		
		assertEquals(expected, PropertiesUtils.deserialize("xxx=1\nkkk = 2\ninclude=file.properties"));
		
		try (InputStream is = getClass().getResourceAsStream("PropertiesUtilsTest.properties")) {
			byte[] bytes = IOUtils.toByteArray(is);
			String s = new String(bytes, "UTF-8");
			Map<String, String> actual = PropertiesUtils.deserialize(s);
			
			// test some special cases first
			assertEquals("separator", actual.get("alternative"));
			assertEquals("as a separator", actual.get("space"));
			assertEquals("escaped:colon:value", actual.get("escaped:colon:key"));
			assertEquals("value with spaces", actual.get("key with spaces"));
			assertEquals("Hello world!", actual.get("exclamation.mark"));
			assertEquals("http://en.wikipedia.org/", actual.get("website"));
			assertEquals("include is just a normal property", actual.get("include"));
			
			// compare the result with java.util.Properties
			Properties properties = new Properties();
			try (InputStream bais = new ByteArrayInputStream(bytes)) {
				properties.load(bais);
				assertEquals(diff(properties, actual), properties, actual);
			}
		}
		
		// the returned map must preserve insertion iteration order
		Map<String, String> actual = PropertiesUtils.deserialize("key=value");
		actual.clear(); // start with an empty map
		testOrder(actual);

		// test map returned by deserialize(null)
		testOrder(PropertiesUtils.deserialize(null));

		// test map returned by deserialize("")
		testOrder(PropertiesUtils.deserialize(""));
	}
	
	/*
	 * Helper method to test insertion iteration order and modifiability
	 */
	private void testOrder(Map<String, String> map) {
		for (int i = 0; i < 10; i++) {
			String s = String.valueOf(i);
			map.put(s, s); // modifiable
		}
		int previous = -1;
		assertEquals(10, map.size());
		for (Iterator<String> it = map.keySet().iterator(); it.hasNext(); ) {
			String s = it.next();
			int i = Integer.parseInt(s);
			assertTrue(i == previous + 1);
			previous = i;
		}
	}
	
	public void testSerialize() throws IOException {
		Map<String, String> map = new HashMap<>();
		map.put("key", "value");
		// no date should be printed
		assertEquals("key=value", PropertiesUtils.serialize(map).trim());
		
		// empty map should be serialized to an empty string
		assertTrue(PropertiesUtils.serialize(Collections.<String, String>emptyMap()).isEmpty());
		// null should also be serialized to an empty string
		assertEquals("", PropertiesUtils.serialize(null));
		
		/*
		 *  The serialized format should be the same as for Properties with the following exceptions:
		 *  - no date should be written
		 *  - national characters should not be escaped (like in Properties.store(Writer, String))
		 */
		try (InputStream is = getClass().getResourceAsStream("PropertiesUtilsTest.properties")) {
			byte[] bytes = IOUtils.toByteArray(is);
			// compare the result with java.util.Properties
			Properties properties = new Properties();
			try (InputStream bais = new ByteArrayInputStream(bytes)) {
				properties.load(bais);
			}
			Map<String, String> p = new HashMap<>(properties.size());
			for (String key: properties.stringPropertyNames()) {
				p.put(key, properties.getProperty(key));
			}
			// serialize Properties loaded from "PropertiesUtilsTest.properties"
			String s = PropertiesUtils.serialize(p);
			
			Properties p2 = new Properties();
			try (StringReader reader = new StringReader(s)) {
				p2.load(reader);
			}
			
			assertEquals(properties, p2); // load as Properties
			assertEquals(properties, PropertiesUtils.deserialize(s)); // deserialize
		}
		
		// "Prilis zlutoucky kun..."
		String kun = "P\u0159\u00EDli\u0161 \u017Elu\u0165ou\u010Dk\u00FD k\u016F\u0148 \u00FAp\u011Bl \u010F\u00E1belsk\u00E9 \u00F3dy.";
		map.clear();
		map.put(kun, kun);
		// National characters should NOT be escaped in the serialized string:
		assertEquals(kun.replace(" ", "\\ ") + "=" + kun, PropertiesUtils.serialize(map).trim());
	}
	
}

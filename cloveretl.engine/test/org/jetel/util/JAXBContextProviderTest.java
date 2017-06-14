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
package org.jetel.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import javax.xml.bind.JAXBContext;

import org.jetel.graph.dictionary.jaxb.Dictionary;
import org.jetel.graph.dictionary.jaxb.Property;
import org.junit.Test;

/**
 * @author reichman (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jun 12, 2017
 */
public class JAXBContextProviderTest {

	private JAXBContextProvider provider = JAXBContextProvider.getInstance();
	
	private final static String CONTEXT_PATH = "org.jetel.graph.dictionary.jaxb";
			
	@Test
	public void testSameClassLoader1() throws Exception {
			
		JAXBContext context1 = provider.getContext(CONTEXT_PATH);
		JAXBContext context2 = provider.getContext(CONTEXT_PATH);
		
		assertEquals(context1, context2);
	}
	
	@Test
	public void testSameClassLoader2() throws Exception {
			
		ClassLoader classLoader = new ClassLoader() {};
		
		JAXBContext context1 = provider.getContext(CONTEXT_PATH, classLoader);
		JAXBContext context2 = provider.getContext(CONTEXT_PATH, classLoader);
		
		assertEquals(context1, context2);
	}
	
	@Test
	public void testSameClassLoader3() throws Exception {
			
		ClassLoader classLoader = new ClassLoader() {};
		
		JAXBContext context1 = provider.getContext(CONTEXT_PATH, classLoader);
		
		Thread.currentThread().setContextClassLoader(classLoader);
		JAXBContext context2 = provider.getContext(CONTEXT_PATH);
		
		assertEquals(context1, context2);
	}
	
	@Test
	public void testSameClassLoader4() throws Exception {
			
		ClassLoader classLoader = new ClassLoader() {};
		
		Thread.currentThread().setContextClassLoader(classLoader);
		
		JAXBContext context1 = provider.getContext(CONTEXT_PATH, classLoader);
		JAXBContext context2 = provider.getContext(CONTEXT_PATH, classLoader);
		
		assertEquals(context1, context2);
	}
	
	@Test
	public void testSameClassLoader5() throws Exception {
			
		ClassLoader classLoader = new ClassLoader() {};
		
		Thread.currentThread().setContextClassLoader(classLoader);
		
		JAXBContext context1 = provider.getContext(CONTEXT_PATH);
		JAXBContext context2 = provider.getContext(CONTEXT_PATH);
		
		assertEquals(context1, context2);
	}
	
	@Test
	public void testDifferentClassLoader1() throws Exception {
			
		JAXBContext context1 = provider.getContext(CONTEXT_PATH);
		
		Thread.currentThread().setContextClassLoader(new ClassLoader() {});
		JAXBContext context2 = provider.getContext(CONTEXT_PATH);
		
		assertNotEquals(context1, context2);
	}
	
	@Test
	public void testDifferentClassLoader2() throws Exception {
			
		JAXBContext context1 = provider.getContext(CONTEXT_PATH, new ClassLoader() {});
		JAXBContext context2 = provider.getContext(CONTEXT_PATH, new ClassLoader() {});
		
		assertNotEquals(context1, context2);
	}
	
	@Test
	public void testDifferentClassLoader3() throws Exception {
			
		JAXBContext context1 = provider.getContext(CONTEXT_PATH, new ClassLoader() {});
		
		Thread.currentThread().setContextClassLoader(new ClassLoader() {});
		JAXBContext context2 = provider.getContext(CONTEXT_PATH);
		
		assertNotEquals(context1, context2);
	}
	
	@Test
	public void testDifferentClassLoader4() throws Exception {
			
		JAXBContext context1 = provider.getContext(CONTEXT_PATH);
		JAXBContext context2 = provider.getContext(CONTEXT_PATH, new ClassLoader() {});
		
		assertNotEquals(context1, context2);
	}
		
	@Test
	public void testTypesEquals() throws Exception {
			
		JAXBContext context1 = provider.getContext(new Class[]{Dictionary.class});
		JAXBContext context2 = provider.getContext(new Class[]{Dictionary.class});
		
		assertEquals(context1, context2);
	}
	
	@Test
	public void testTypesNotEquals() throws Exception {
			
		JAXBContext context1 = provider.getContext(new Class[]{Dictionary.class});
		JAXBContext context2 = provider.getContext(new Class[]{Property.class});
		
		assertNotEquals(context1, context2);
	}
	
	@Test
	public void testTypesEqualsDfferentOrder() throws Exception {
			
		JAXBContext context1 = provider.getContext(new Class[]{Dictionary.class, Property.class});
		JAXBContext context2 = provider.getContext(new Class[]{Property.class, Dictionary.class});
		
		assertEquals(context1, context2);
	}
	
	@Test
	public void testTypesEqualsDifferentClassLoader() throws Exception {
			
		JAXBContext context1 = provider.getContext(new Class[]{Dictionary.class});
		Thread.currentThread().setContextClassLoader(new ClassLoader() {});
		JAXBContext context2 = provider.getContext(new Class[]{Dictionary.class});
		
		assertEquals(context1, context2);
	}
	
	@Test(expected=NullPointerException.class)
	public void testTypesNull() throws Exception {
		provider.getContext((Class[])null);
	}
	
	@Test(expected=NullPointerException.class)
	public void testNullClassLoader() throws Exception {
		provider.getContext(CONTEXT_PATH, null);
	}
}

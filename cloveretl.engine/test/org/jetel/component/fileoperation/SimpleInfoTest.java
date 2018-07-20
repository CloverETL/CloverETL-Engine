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
package org.jetel.component.fileoperation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Date;

import org.jetel.component.fileoperation.Info.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14. 6. 2018
 */
public class SimpleInfoTest {
	
	private SimpleInfo info;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.info = new SimpleInfo("myFile.txt", URI.create("ftp://user:password@hostname/dir/myFile.txt"));
		setDefaultValues(info);
	}

	private void setDefaultValues(SimpleInfo instance) {
		instance.setParentDir(URI.create("ftp://user:password@hostname/dir/"));
		instance.setType(Type.FILE);
		instance.setLastModified(new Date(1));
		instance.setLastAccessed(new Date(2));
		instance.setCreated(new Date(3));
		instance.setCanRead(true);
		instance.setCanWrite(true);
		instance.setCanExecute(true);
		instance.setSize(50);
		instance.setHidden(false);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		this.info = null;
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#SimpleInfo(java.lang.String, java.net.URI)}.
	 */
	@Test
	public void testConstructorStringURI() {
		SimpleInfo newInstance = new SimpleInfo("myFile.txt", URI.create("ftp://user:password@hostname/dir/myFile.txt"));
		setDefaultValues(newInstance);
		checkDefaultValues(newInstance);
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#SimpleInfo(org.jetel.component.fileoperation.Info)}.
	 */
	@Test
	public void testConstructorInfo() {
		Info copy = new SimpleInfo(info);
		checkDefaultValues(copy);
	}

	private void checkDefaultValues(Info instance) {
		assertEquals("myFile.txt", instance.getName());
		assertEquals(URI.create("ftp://user:password@hostname/dir/myFile.txt"), instance.getURI());
		assertEquals(URI.create("ftp://user:password@hostname/dir/"), instance.getParentDir());
		assertEquals(Type.FILE, instance.getType());
		assertEquals(new Date(1), instance.getLastModified());
		assertEquals(new Date(2), instance.getLastAccessed());
		assertEquals(new Date(3), instance.getCreated());
		assertTrue(instance.canRead());
		assertTrue(instance.canWrite());
		assertTrue(instance.canExecute());
		assertEquals(new Long(50), instance.getSize());
		assertFalse(instance.isHidden());
		assertTrue(instance.isFile());
		assertFalse(instance.isLink());
		assertFalse(instance.isDirectory());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#getName()}.
	 */
	@Test
	public void testGetName() {
		assertEquals("myFile.txt", info.getName());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#getURI()}.
	 */
	@Test
	public void testGetURI() {
		assertEquals(URI.create("ftp://user:password@hostname/dir/myFile.txt"), info.getURI());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#getParentDir()}.
	 */
	@Test
	public void testGetParentDir() {
		assertEquals(URI.create("ftp://user:password@hostname/dir/"), info.getParentDir());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#isDirectory()}.
	 */
	@Test
	public void testIsDirectory() {
		assertFalse(info.isDirectory());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#getSize()}.
	 */
	@Test
	public void testGetSize() {
		assertEquals(Long.valueOf(50), info.getSize());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#getLastModified()}.
	 */
	@Test
	public void testGetLastModified() {
		assertEquals(new Date(1), info.getLastModified());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setType(org.jetel.component.fileoperation.Info.Type)}.
	 */
	@Test
	public void testSetType() {
		info.setType(Type.DIR);
		assertEquals(Type.DIR, info.getType());
		info.setType(null);
		assertNull(info.getType());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setSize(long)}.
	 */
	@Test
	public void testSetSize() {
		info.setSize(123456);
		assertEquals(Long.valueOf(123456), info.getSize());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setCreated(java.util.Date)}.
	 */
	@Test
	public void testSetCreated() {
		info.setCreated(new Date(123456));
		assertEquals(new Date(123456), info.getCreated());
		info.setCreated(null);
		assertNull(info.getCreated());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setLastModified(java.util.Date)}.
	 */
	@Test
	public void testSetLastModified() {
		info.setLastModified(new Date(123456));
		assertEquals(new Date(123456), info.getLastModified());
		info.setLastModified(null);
		assertNull(info.getLastModified());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setLastAccessed(java.util.Date)}.
	 */
	@Test
	public void testSetLastAccessed() {
		info.setLastAccessed(new Date(123456));
		assertEquals(new Date(123456), info.getLastAccessed());
		info.setLastAccessed(null);
		assertNull(info.getLastAccessed());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setParentDir(java.net.URI)}.
	 */
	@Test
	public void testSetParentDir() {
		info.setParentDir(URI.create("invalid"));
		assertEquals(URI.create("invalid"), info.getParentDir());
		info.setParentDir(null);
		assertNull(info.getParentDir());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#isFile()}.
	 */
	@Test
	public void testIsFile() {
		assertTrue(info.isFile());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#isLink()}.
	 */
	@Test
	public void testIsLink() {
		assertFalse(info.isLink());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#getType()}.
	 */
	@Test
	public void testGetType() {
		assertEquals(Type.FILE, info.getType());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#getCreated()}.
	 */
	@Test
	public void testGetCreated() {
		assertEquals(new Date(3), info.getCreated());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#getLastAccessed()}.
	 */
	@Test
	public void testGetLastAccessed() {
		assertEquals(new Date(2), info.getLastAccessed());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#toString()}.
	 */
	@Test
	public void testToString() {
		assertEquals("ftp://user:password@hostname/dir/myFile.txt", info.toString());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#isHidden()}.
	 */
	@Test
	public void testIsHidden() {
		assertFalse(info.isHidden());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#canRead()}.
	 */
	@Test
	public void testCanRead() {
		assertTrue(info.canRead());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#canWrite()}.
	 */
	@Test
	public void testCanWrite() {
		assertTrue(info.canWrite());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#canExecute()}.
	 */
	@Test
	public void testCanExecute() {
		assertTrue(info.canExecute());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setCanRead(boolean)}.
	 */
	@Test
	public void testSetCanRead() {
		info.setCanRead(true);
		assertTrue(info.canRead());
		info.setCanRead(false);
		assertFalse(info.canRead());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setCanWrite(boolean)}.
	 */
	@Test
	public void testSetCanWrite() {
		info.setCanWrite(true);
		assertTrue(info.canWrite());
		info.setCanWrite(false);
		assertFalse(info.canWrite());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setCanExecute(boolean)}.
	 */
	@Test
	public void testSetCanExecute() {
		info.setCanExecute(true);
		assertTrue(info.canExecute());
		info.setCanExecute(false);
		assertFalse(info.canExecute());
	}

	/**
	 * Test method for {@link org.jetel.component.fileoperation.SimpleInfo#setHidden(boolean)}.
	 */
	@Test
	public void testSetHidden() {
		info.setHidden(true);
		assertTrue(info.isHidden());
		info.setHidden(false);
		assertFalse(info.isHidden());
	}

}

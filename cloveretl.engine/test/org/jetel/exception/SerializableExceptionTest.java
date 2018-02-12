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
package org.jetel.exception;

import java.io.IOException;
import java.util.Arrays;

import org.jetel.test.CloverTestCase;
import org.jetel.util.ExceptionUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.6.2013
 */
public class SerializableExceptionTest extends CloverTestCase {

	public void test1() {
		Exception e = new NullPointerException();
		SerializableException se = new SerializableException(e);
		
		compare(e, se);
		
		assertTrue(ExceptionUtils.instanceOf(se, NullPointerException.class));
		assertFalse(ExceptionUtils.instanceOf(se, SerializableException.class));
		assertFalse(ExceptionUtils.instanceOf(se, JetelRuntimeException.class));
		assertFalse(ExceptionUtils.instanceOf(se, IOException.class));
	}

	public void test2() {
		Exception e = new NullPointerException("xxx");
		SerializableException se = new SerializableException(e);
		
		compare(e, se);
	}

	public void test3() {
		Exception e = new NullPointerException("xxx");
		e = new JetelRuntimeException(e);
		SerializableException se = new SerializableException(e);
		
		compare(e, se);

		assertTrue(ExceptionUtils.instanceOf(se, NullPointerException.class));
		assertFalse(ExceptionUtils.instanceOf(se, SerializableException.class));
		assertTrue(ExceptionUtils.instanceOf(se, JetelRuntimeException.class));
		assertFalse(ExceptionUtils.instanceOf(se, IOException.class));
	}

	public void test4() {
		Exception e = new NullPointerException("xxx");
		e = new JetelRuntimeException("yyy", e);
		SerializableException se = new SerializableException(e);
		
		compare(e, se);
	}

	public void test5() {
		Exception e = new NullPointerException("xxx");
		e = new JetelRuntimeException("yyy", e);
		e = new JetelRuntimeException("zzz", e);
		SerializableException se = new SerializableException(e);
		
		compare(e, se);

		assertTrue(ExceptionUtils.instanceOf(se, NullPointerException.class));
		assertFalse(ExceptionUtils.instanceOf(se, SerializableException.class));
		assertTrue(ExceptionUtils.instanceOf(se, JetelRuntimeException.class));
		assertFalse(ExceptionUtils.instanceOf(se, IOException.class));
	}

	public void test6() {
		Exception e = new NullPointerException("xxx");
		e = new JetelRuntimeException(e);
		e = new JetelRuntimeException("zzz", e);
		SerializableException se = new SerializableException(e);
		
		compare(e, se);
	}

	public void test7() {
		Exception e = new NullPointerException();
		e = new JetelRuntimeException("yyy", e);
		e = new JetelRuntimeException("zzz", e);
		SerializableException se = new SerializableException(e);
		
		compare(e, se);
	}

	public void test8() {
		Exception e = new NullPointerException("xxx");
		e = new JetelRuntimeException("yyy", e);
		e = new JetelRuntimeException(e);
		SerializableException se = new SerializableException(e);
		
		compare(e, se);
	}

	public void test9() {
		Exception e = new NullPointerException();
		e = new JetelRuntimeException(e);
		e = new JetelRuntimeException("zzz", e);
		SerializableException se = new SerializableException(e);
		
		compare(e, se);
	}
	
	public void test10() {
		Exception e = new NullPointerException();
		e = new JetelRuntimeException(e);
		e = new JetelRuntimeException(e);
		SerializableException se = new SerializableException(e);
		
		compare(e, se);
	}

	public void test11() {
		Exception e = new CompoundException("xxx",
				new JetelRuntimeException(),
				new NullPointerException(),
				new NullPointerException("yyy"),
				new JetelRuntimeException("zzz"));
		SerializableException se = new SerializableException(e);
		
		compare(e, se);

		assertTrue(ExceptionUtils.instanceOf(se, NullPointerException.class));
		assertFalse(ExceptionUtils.instanceOf(se, SerializableException.class));
		assertTrue(ExceptionUtils.instanceOf(se, JetelRuntimeException.class));
		assertFalse(ExceptionUtils.instanceOf(se, IOException.class));
		assertTrue(ExceptionUtils.instanceOf(se, CompoundException.class));
	}

	public void test12() {
		Throwable e = new Exception("aaa");
		e.addSuppressed(new Exception("bbb"));

		SerializableException se = new SerializableException(e);
		
		compare(e, se);
	}

	public void test13() {
		Throwable e = new Exception("aaa", new JetelRuntimeException("xxx"));
		Exception suppressedEx = new IOException("yyy");
		suppressedEx.addSuppressed(new JetelException("zzz"));
		e.addSuppressed(new Exception("bbb", suppressedEx));
		e.addSuppressed(new Exception("ccc"));

		SerializableException se = new SerializableException(e);
		
		compare(e, se);

		assertFalse(ExceptionUtils.instanceOf(se, NullPointerException.class));
		assertFalse(ExceptionUtils.instanceOf(se, SerializableException.class));
		assertTrue(ExceptionUtils.instanceOf(se, JetelRuntimeException.class));
		assertTrue(ExceptionUtils.instanceOf(se, IOException.class));
		assertFalse(ExceptionUtils.instanceOf(se, CompoundException.class));
		assertTrue(ExceptionUtils.instanceOf(se, JetelException.class));
	}

	public void test14() {
		Throwable e = new Exception("aaa", new JetelRuntimeException("xxx"));
		Exception suppressedEx = new IOException("yyy");
		suppressedEx.addSuppressed(new JetelException("zzz"));
		suppressedEx.addSuppressed(new CompoundException("www", new NullPointerException("qqq"), new IllegalArgumentException()));
		e.addSuppressed(new Exception("bbb", suppressedEx));
		e.addSuppressed(new Exception("ccc"));

		SerializableException se = new SerializableException(e);
		
		compare(e, se);

		assertTrue(ExceptionUtils.instanceOf(se, NullPointerException.class));
		assertFalse(ExceptionUtils.instanceOf(se, SerializableException.class));
		assertTrue(ExceptionUtils.instanceOf(se, JetelRuntimeException.class));
		assertTrue(ExceptionUtils.instanceOf(se, IOException.class));
		assertTrue(ExceptionUtils.instanceOf(se, CompoundException.class));
		assertTrue(ExceptionUtils.instanceOf(se, JetelException.class));
		assertTrue(ExceptionUtils.instanceOf(se, CompoundException.class));
		assertTrue(ExceptionUtils.instanceOf(se, IllegalArgumentException.class));
	}

	private void compare(Throwable e, SerializableException se) {
		assertEquals(e.getLocalizedMessage(), se.getLocalizedMessage());
		assertEquals(e.getMessage(), se.getMessage());
		assertEquals(e.toString(), se.toString());
		assertTrue(Arrays.equals(e.getStackTrace(), se.getStackTrace()));
		assertEquals(ExceptionUtils.getMessage(e), ExceptionUtils.getMessage(se));
		assertEquals(ExceptionUtils.stackTraceToString(e), ExceptionUtils.stackTraceToString(se));

		Throwable[] suppressedException = e.getSuppressed();
		Throwable[] serializableSuppressedException = se.getSuppressed();
		assertTrue(suppressedException.length == serializableSuppressedException.length);
		for (int i = 0; i < suppressedException.length; i++) {
			compare(suppressedException[i], (SerializableException) serializableSuppressedException[i]);
		}
		
		assertTrue((e.getCause() != null && se.getCause() != null) || (e.getCause() == null && se.getCause() == null));
		
		if (e.getCause() != null) {
			compare(e.getCause(), se.getCause());
		}
		
		assertTrue(se.instanceOf(e.getClass()));
		assertTrue(e.getClass().getName().equals(se.getWrappedExceptionClassName()));
	}
	
}

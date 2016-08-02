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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jetel.exception.CompoundException;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.MissingFieldException;
import org.jetel.exception.StackTraceWrapperException;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.2.2013
 */
public class ExceptionUtilsTest extends CloverTestCase {
	
	public void testGetMessage() {
		assertEquals("", ExceptionUtils.getMessage(null, null));
		
		assertEquals("", ExceptionUtils.getMessage("", null));
		
		assertEquals("abc", ExceptionUtils.getMessage("abc", null));
		
		assertEquals("java.lang.Exception", ExceptionUtils.getMessage(null, new Exception()));

		assertEquals("java.lang.Exception", ExceptionUtils.getMessage("", new Exception()));

		assertEquals("abc\n java.lang.Exception", ExceptionUtils.getMessage("abc", new Exception()));

		assertEquals("first", ExceptionUtils.getMessage(null, new Exception("first")));

		assertEquals("first", ExceptionUtils.getMessage("", new Exception("first")));
		
		assertEquals("abc\n first", ExceptionUtils.getMessage("abc", new Exception("first")));

		assertEquals("abc\n first\n  java.lang.Exception", ExceptionUtils.getMessage("abc", new Exception("first", new Exception())));

		assertEquals("abc\n first\n  java.lang.Exception: ", ExceptionUtils.getMessage("abc", new Exception("first", new Exception(""))));

		assertEquals("abc\n first", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first"))));

		assertEquals("abc\n first\n  second", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("second"))));

		assertEquals("abc\n first\n  second", ExceptionUtils.getMessage("abc", new Exception("first", new Exception(null, new Exception("second")))));

		assertEquals("abc\n first\n  second", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("", new Exception("second")))));

		assertEquals("abc\n first\n  second", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new Exception("second")))));

		assertEquals("abc\n first\n  java.lang.Exception", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException(new Exception())))));

		assertEquals("abc\n first\n  java.lang.Exception", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException("", new Exception())))));

		assertEquals("abc\n first\n  second", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException(new Exception("second"))))));

		assertEquals("abc\n first\n  second\n   java.lang.Exception", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception())))));

		assertEquals("abc\n first\n  second\n   java.lang.Exception: ", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception(""))))));
		
		assertEquals("abc\n first\n  second\n   third", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception("third"))))));

		assertEquals("abc\n first\n  second\n   third\n   forth", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception("third"), new Exception("forth"))))));

		assertEquals("abc\n first\n  third\n  forth", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException(new Exception("third"), new Exception("forth"))))));

		assertEquals("abc\n first\n  second\n   java.lang.Exception: \n   forth", ExceptionUtils.getMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception(""), new Exception("forth"))))));

		assertEquals("abc\n first\n  second\n  second2\n   third\n    third1\n    third2\n     third3\n   forth", 
				ExceptionUtils.getMessage("abc", 
						new Exception("first", 
								new Exception("first", 
										new CompoundException("second\nsecond2", 
												new CompoundException("third", new Exception("third1"), new Exception("third2", new Exception("third3"))), 
												new Exception("forth"))))));

		assertEquals("Unexpected null value.", ExceptionUtils.getMessage(null, new NullPointerException()));
		
		assertEquals("abc\n Unexpected null value.", ExceptionUtils.getMessage("abc", new NullPointerException()));

		assertEquals("abc\n first", ExceptionUtils.getMessage("abc", new NullPointerException("first")));

		assertEquals("abc\n Unexpected null value.", ExceptionUtils.getMessage("abc", new NullPointerException("")));

		assertEquals("abc\n Unexpected null value.", ExceptionUtils.getMessage("abc", new NullPointerException("null")));

		assertEquals("abc\n first\n  Unexpected null value.", ExceptionUtils.getMessage("abc", new Exception("first", new NullPointerException("null"))));

		assertEquals("abc\n java.lang.Exception", ExceptionUtils.getMessage("abc", new Exception()));

		assertEquals("abc\n java.lang.Exception", ExceptionUtils.getMessage("abc", new Exception(new Exception())));
		
		assertEquals("abc\n first", ExceptionUtils.getMessage("abc", new Exception(new Exception("first"))));

		assertEquals("abc\n first", ExceptionUtils.getMessage("abc", new Exception(new NullPointerException("first"))));

		assertEquals("abc\n Unexpected null value.", ExceptionUtils.getMessage("abc", new Exception(new NullPointerException())));

		assertEquals("abc\n first\n  second", ExceptionUtils.getMessage("abc", new Exception("first", new Exception(new Exception(new Exception(new Exception("second")))))));

		assertEquals("abc\n Unexpected null value.", ExceptionUtils.getMessage("abc", new Exception(new Exception(new NullPointerException()))));

		assertEquals("abc\n java.lang.Exception", ExceptionUtils.getMessage("abc", new Exception(new Exception(new Exception()))));

		assertEquals("Class with the specified name cannot be found: a.b.c\n No definition for the class with the specified name can be found: a/b/c", ExceptionUtils.getMessage(new ClassNotFoundException("a.b.c", new NoClassDefFoundError("a/b/c"))));

		
		Exception e = new Exception();
		e.addSuppressed(new Exception());
		assertEquals("java.lang.Exception\n Suppressed: java.lang.Exception", ExceptionUtils.getMessage(e));

		e = new Exception("abc");
		e.addSuppressed(new Exception());
		assertEquals("abc\n Suppressed: java.lang.Exception", ExceptionUtils.getMessage(e));
		assertEquals("abc\n Suppressed: java.lang.Exception", ExceptionUtils.getMessage(new Exception("abc", e)));

		e = new Exception("abc");
		e.addSuppressed(new Exception(new Exception("def")));
		assertEquals("abc\n Suppressed: def", ExceptionUtils.getMessage(e));

		e = new Exception("aaa");
		e.addSuppressed(new Exception(new Exception("bbb")));
		e.addSuppressed(new Exception(new Exception("ccc")));
		assertEquals("aaa\n Suppressed: bbb\n Suppressed: ccc", ExceptionUtils.getMessage(e));

		e = new Exception("aaa");
		e.addSuppressed(new Exception(new Exception("bbb")));
		e.addSuppressed(new Exception("ccc", new Exception("ccc")));
		assertEquals("aaa\n Suppressed: bbb\n Suppressed: ccc", ExceptionUtils.getMessage(e));

		e = new Exception("aaa");
		e.addSuppressed(new Exception(new Exception("bbb")));
		e.addSuppressed(new Exception("ccc", new Exception("ddd")));
		assertEquals("aaa\n Suppressed: bbb\n Suppressed: ccc\n  ddd", ExceptionUtils.getMessage(e));

		e = new Exception("aaa");
		e.addSuppressed(new Exception(new Exception("bbb")));
		e.addSuppressed(new Exception("ccc", new CompoundException(new Exception("ddd"), new Exception())));
		assertEquals("aaa\n Suppressed: bbb\n Suppressed: ccc\n  ddd\n  java.lang.Exception", ExceptionUtils.getMessage(e));

		e = new Exception("aaa");
		e.addSuppressed(new Exception(new Exception("bbb")));
		e.addSuppressed(new Exception("ccc", new CompoundException("ddd", new Exception("eee"), new Exception())));
		assertEquals("aaa\n Suppressed: bbb\n Suppressed: ccc\n  ddd\n   eee\n   java.lang.Exception", ExceptionUtils.getMessage(e));

		e = new Exception("aaa", new Exception("aaaa", new Exception()));
		e.addSuppressed(new Exception(new Exception("bbb")));
		e.addSuppressed(new Exception("ccc", new CompoundException("ddd", new Exception("eee"), new Exception())));
		assertEquals("aaa\n aaaa\n  java.lang.Exception\n Suppressed: bbb\n Suppressed: ccc\n  ddd\n   eee\n   java.lang.Exception", ExceptionUtils.getMessage(e));

		e = new Exception("aaa", new Exception("aaaa", new Exception()));
		e.addSuppressed(new Exception(new Exception("bbb")));
		CompoundException ce = new CompoundException("ddd", new Exception("eee"), new Exception());
		ce.addSuppressed(new Exception("ce"));
		e.addSuppressed(new Exception("ccc", ce));
		assertEquals("aaa\n aaaa\n  java.lang.Exception\n Suppressed: bbb\n Suppressed: ccc\n  ddd\n   eee\n   java.lang.Exception\n   Suppressed: ce", ExceptionUtils.getMessage(e));

		e = new Exception("second", new Exception("third"));
		e.addSuppressed(new Exception("suppressed"));
		assertEquals("abc\n first\n  second\n   third\n   Suppressed: suppressed", ExceptionUtils.getMessage("abc", new Exception("first", e)));
	}

	public void testInstanceOf() {
		assertFalse(ExceptionUtils.instanceOf(null, JetelRuntimeException.class));
		assertTrue(ExceptionUtils.instanceOf(new JetelRuntimeException(), JetelRuntimeException.class));
		assertFalse(ExceptionUtils.instanceOf(new JetelRuntimeException(), MissingFieldException.class));
		assertTrue(ExceptionUtils.instanceOf(new StackTraceWrapperException("", ""), JetelRuntimeException.class));
		assertFalse(ExceptionUtils.instanceOf(new NullPointerException(), JetelRuntimeException.class));
		assertTrue(ExceptionUtils.instanceOf(new RuntimeException(new JetelRuntimeException()), JetelRuntimeException.class));
		assertTrue(ExceptionUtils.instanceOf(new RuntimeException(new JetelRuntimeException(new NullPointerException())), JetelRuntimeException.class));
		assertFalse(ExceptionUtils.instanceOf(new RuntimeException(new JetelRuntimeException(new NullPointerException())), StackTraceWrapperException.class));
		assertFalse(ExceptionUtils.instanceOf(new CompoundException(new JetelRuntimeException(), new NullPointerException()), StackTraceWrapperException.class));
		assertTrue(ExceptionUtils.instanceOf(new CompoundException(new JetelRuntimeException(), new NullPointerException()), JetelRuntimeException.class));
		assertTrue(ExceptionUtils.instanceOf(new CompoundException(new JetelRuntimeException(), new NullPointerException()), NullPointerException.class));
		assertTrue(ExceptionUtils.instanceOf(new RuntimeException(new CompoundException(new JetelRuntimeException(), new NullPointerException())), NullPointerException.class));
		assertTrue(ExceptionUtils.instanceOf(new RuntimeException(new CompoundException(new JetelRuntimeException(), new NullPointerException())), RuntimeException.class));
		
		
		Exception e = new JetelRuntimeException();
		e.addSuppressed(new IllegalArgumentException(new JetelException("a")));
		assertTrue(ExceptionUtils.instanceOf(new RuntimeException(new CompoundException(e, new NullPointerException())), IllegalArgumentException.class));
		assertTrue(ExceptionUtils.instanceOf(new RuntimeException(new CompoundException(e, new NullPointerException())), JetelException.class));
		assertFalse(ExceptionUtils.instanceOf(new RuntimeException(new CompoundException(e, new NullPointerException())), StackTraceWrapperException.class));
	}
	
	public void testGetAllExceptions() {
		List<Throwable> result = new ArrayList<Throwable>();
		Exception e;
		
		result.clear();
		assertEquals(result, ExceptionUtils.getAllExceptions(null, RuntimeException.class));
		
		result.clear();
		result.add(new RuntimeException());
		assertEquals(result, ExceptionUtils.getAllExceptions(result.get(0), RuntimeException.class));
		
		result.clear();
		assertEquals(result, ExceptionUtils.getAllExceptions(new RuntimeException(), JetelRuntimeException.class));

		result.clear();
		e = new RuntimeException(new JetelRuntimeException());
		result.add(e.getCause());
		assertEquals(result, ExceptionUtils.getAllExceptions(e, JetelRuntimeException.class));

		result.clear();
		e = new RuntimeException(new JetelRuntimeException(new JetelRuntimeException()));
		result.add(e.getCause());
		result.add(e.getCause().getCause());
		assertEquals(result, ExceptionUtils.getAllExceptions(e, JetelRuntimeException.class));

		result.clear();
		e = new RuntimeException(new JetelRuntimeException(new JetelRuntimeException(new RuntimeException(new JetelRuntimeException()))));
		result.add(e.getCause());
		result.add(e.getCause().getCause());
		result.add(e.getCause().getCause().getCause().getCause());
		assertEquals(result, ExceptionUtils.getAllExceptions(e, JetelRuntimeException.class));

		result.clear();
		e = new RuntimeException(new JetelRuntimeException(new CompoundException(new RuntimeException(new JetelRuntimeException()), new JetelRuntimeException())));
		result.add(e.getCause());
		result.add(e.getCause().getCause());
		result.add(((CompoundException) e.getCause().getCause()).getCauses().get(0).getCause());
		result.add(((CompoundException) e.getCause().getCause()).getCauses().get(1));
		assertEquals(result, ExceptionUtils.getAllExceptions(e, JetelRuntimeException.class));

		result.clear();
		e = new RuntimeException(new IOException(new CompoundException(new RuntimeException(new IOException()), new IOException())));
		result.add(e.getCause());
		result.add(((CompoundException) e.getCause().getCause()).getCauses().get(0).getCause());
		result.add(((CompoundException) e.getCause().getCause()).getCauses().get(1));
		assertEquals(result, ExceptionUtils.getAllExceptions(e, IOException.class));
	}
	
}

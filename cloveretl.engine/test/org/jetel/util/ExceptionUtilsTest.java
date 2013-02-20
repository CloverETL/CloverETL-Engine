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

import org.jetel.exception.CompoundException;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.2.2013
 */
public class ExceptionUtilsTest extends CloverTestCase {
	
	@Override
	protected void setUp() throws Exception {
		initEngine();
	}

	public void testExceptionChainToMessage() {
		assertEquals("", ExceptionUtils.exceptionChainToMessage(null, null));
		
		assertEquals("", ExceptionUtils.exceptionChainToMessage("", null));
		
		assertEquals("abc", ExceptionUtils.exceptionChainToMessage("abc", null));
		
		assertEquals("", ExceptionUtils.exceptionChainToMessage(null, new Exception()));

		assertEquals("", ExceptionUtils.exceptionChainToMessage("", new Exception()));

		assertEquals("abc", ExceptionUtils.exceptionChainToMessage("abc", new Exception()));

		assertEquals("first", ExceptionUtils.exceptionChainToMessage(null, new Exception("first")));

		assertEquals("first", ExceptionUtils.exceptionChainToMessage("", new Exception("first")));
		
		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first")));

		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception())));

		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception(""))));

		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first"))));

		assertEquals("abc\n first\n  second", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("second"))));

		assertEquals("abc\n first\n  second", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception(null, new Exception("second")))));

		assertEquals("abc\n first\n  second", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("", new Exception("second")))));

		assertEquals("abc\n first\n  second", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new Exception("second")))));

		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException(new Exception())))));

		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException("", new Exception())))));

		assertEquals("abc\n first\n  second", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException(new Exception("second"))))));

		assertEquals("abc\n first\n  second", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception())))));

		assertEquals("abc\n first\n  second", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception(""))))));
		
		assertEquals("abc\n first\n  second\n   third", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception("third"))))));

		assertEquals("abc\n first\n  second\n   third\n   forth", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception("third"), new Exception("forth"))))));

		assertEquals("abc\n first\n  third\n  forth", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException(new Exception("third"), new Exception("forth"))))));

		assertEquals("abc\n first\n  second\n   forth", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception("first", new CompoundException("second", new Exception(""), new Exception("forth"))))));

		assertEquals("abc\n first\n  second\n  second2\n   third\n    third1\n    third2\n     third3\n   forth", 
				ExceptionUtils.exceptionChainToMessage("abc", 
						new Exception("first", 
								new Exception("first", 
										new CompoundException("second\nsecond2", 
												new CompoundException("third", new Exception("third1"), new Exception("third2", new Exception("third3"))), 
												new Exception("forth"))))));

		assertEquals("Unexpected null value.", ExceptionUtils.exceptionChainToMessage(null, new NullPointerException()));
		
		assertEquals("abc\n Unexpected null value.", ExceptionUtils.exceptionChainToMessage("abc", new NullPointerException()));

		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new NullPointerException("first")));

		assertEquals("abc\n Unexpected null value.", ExceptionUtils.exceptionChainToMessage("abc", new NullPointerException("")));

		assertEquals("abc\n Unexpected null value.", ExceptionUtils.exceptionChainToMessage("abc", new NullPointerException("null")));

		assertEquals("abc\n first\n  Unexpected null value.", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new NullPointerException("null"))));

		assertEquals("abc", ExceptionUtils.exceptionChainToMessage("abc", new Exception()));

		assertEquals("abc", ExceptionUtils.exceptionChainToMessage("abc", new Exception(new Exception())));
		
		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new Exception(new Exception("first"))));

		assertEquals("abc\n first", ExceptionUtils.exceptionChainToMessage("abc", new Exception(new NullPointerException("first"))));

		assertEquals("abc\n Unexpected null value.", ExceptionUtils.exceptionChainToMessage("abc", new Exception(new NullPointerException())));

		assertEquals("abc\n first\n  second", ExceptionUtils.exceptionChainToMessage("abc", new Exception("first", new Exception(new Exception(new Exception(new Exception("second")))))));

		assertEquals("abc\n Unexpected null value.", ExceptionUtils.exceptionChainToMessage("abc", new Exception(new Exception(new NullPointerException()))));
	}

}

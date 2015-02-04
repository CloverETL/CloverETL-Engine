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
package org.jetel.util.formatter;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import org.jetel.ctl.TransformLangExecutor;
import org.jetel.test.CloverTestCase;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Apr 2, 2013
 */
public class NumericFormatterFactoryTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	public void testGetDecimalFormatter() throws Exception {
		NumericFormatter formatter;
		BigDecimal result;
		String input = "12345678901234567890123456789012345678901234567890.12345678901234567890123456789012345678901234567890";
		MathContext precision = new MathContext(100, RoundingMode.DOWN);
		String locale = "en";
		
		formatter = NumericFormatterFactory.getDecimalFormatter(null, (String) null);
		result = formatter.parseBigDecimal(input);
		assertEquals(new BigDecimal(input, TransformLangExecutor.MAX_PRECISION), result);
		
		formatter = NumericFormatterFactory.getDecimalFormatter(null, (String) null, 100, 50);
		result = formatter.parseBigDecimal(input);
		assertEquals(new BigDecimal(input, precision), result);
		
		formatter = NumericFormatterFactory.getDecimalFormatter(null, locale, 100, 50);
		result = formatter.parseBigDecimal(input);
		assertEquals(new BigDecimal(input, precision), result);
		
		formatter = NumericFormatterFactory.getDecimalFormatter(null, locale, 100, 50);
		result = formatter.parseBigDecimal(input);
		assertEquals(new BigDecimal(input, precision), result);
		
		formatter = NumericFormatterFactory.getDecimalFormatter("#.#", locale, 100, 50);
		result = formatter.parseBigDecimal(input);
		assertEquals(new BigDecimal(input, precision), result);
		
		formatter = NumericFormatterFactory.getDecimalFormatter("#.#", locale, 100, 50);
		result = formatter.parseBigDecimal(input);
		assertEquals(new BigDecimal(input, precision), result);
		
	}

}

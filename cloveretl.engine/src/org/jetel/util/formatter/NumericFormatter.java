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
import java.text.ParseException;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 29, 2010
 */
public interface NumericFormatter {
	
	String getFormatPattern();

	String formatInt(int value);

	int parseInt(CharSequence seq) throws ParseException;

	String formatLong(long value);

	long parseLong(CharSequence seq) throws ParseException;

	String formatDouble(double value);

	double parseDouble(CharSequence seq) throws ParseException;

	String formatBigDecimal(BigDecimal galue);

	BigDecimal parseBigDecimal(CharSequence seq) throws ParseException;

}

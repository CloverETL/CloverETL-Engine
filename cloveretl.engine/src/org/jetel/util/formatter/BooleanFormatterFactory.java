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

import org.jetel.util.string.StringUtils;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 30, 2010
 */
public class BooleanFormatterFactory {

	private static final BooleanFormatter DEFAULT_FORMATTER = new CloverBooleanFormatter();

	public static BooleanFormatter createFormatter(String formatString) {
		if (StringUtils.isEmpty(formatString)) {
			return DEFAULT_FORMATTER;
		} else {
			return new CloverBooleanFormatter(formatString);
		}
	}

	private BooleanFormatterFactory() {
		throw new UnsupportedOperationException();
	}
}

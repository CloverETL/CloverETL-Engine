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

import java.util.Date;
import java.util.Locale;

/**
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 6. 2015
 */
abstract class AbstractDateFormatter implements DateFormatter {
	
	protected String pattern;
	protected Locale locale;
	
	// CLO-5961
	@Override
	public Date parseDateExactMatch(String value) {
		Date parsed = parseDateStrict(value);
		if (parsed != null) {
			String formatted = format(parsed);
			if (!value.equals(formatted)) {
				throw new IllegalArgumentException("Unparseable date: \"" + value + "\" - not exact pattern match");
			}
		}
		return parsed;
	}
	
	@Override
	public String getPattern() {
		return pattern;
	}

	@Override
	public Locale getLocale() {
		return locale;
	}

}

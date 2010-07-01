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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.Defaults;
import org.jetel.data.primitive.StringFormat;
import org.jetel.util.string.MultiPattern;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jun 30, 2010
 */
public class CloverBooleanFormatter implements BooleanFormatter {

	private final StringFormat trueStringFormat;
	private final StringFormat falseStringFormat;
	private final String trueOutputString;
	private final String falseOutputString;

	private final Pattern ESTIMATE_OUTPUT = Pattern.compile("(\\w*)(\\|.*)?");

	public CloverBooleanFormatter() {
		trueStringFormat = StringFormat.create(Defaults.DEFAULT_REGEXP_TRUE_STRING);
		falseStringFormat = StringFormat.create(Defaults.DEFAULT_REGEXP_FALSE_STRING);
		trueOutputString = estimateOutputValue(trueStringFormat, Boolean.TRUE.toString());
		falseOutputString = estimateOutputValue(falseStringFormat, Boolean.FALSE.toString());
	}

	public CloverBooleanFormatter(String formatString) {
		final MultiPattern mp = MultiPattern.parse(formatString);

		trueStringFormat = StringFormat.create(mp.getString(0));

		if (mp.size() >= 2) {
			falseStringFormat = StringFormat.create(mp.getString(1));
		} else {
			falseStringFormat = StringFormat.create(Defaults.DEFAULT_REGEXP_FALSE_STRING);
		}

		if (mp.size() >= 3) {
			trueOutputString = mp.getString(2);
		} else {
			trueOutputString = estimateOutputValue(trueStringFormat, Boolean.TRUE.toString());
		}

		if (mp.size() >= 4) {
			falseOutputString = mp.getString(3);
		} else {
			falseOutputString = estimateOutputValue(falseStringFormat, Boolean.FALSE.toString());
		}

		if (mp.size() >= 5) {
			throw new IllegalArgumentException("Format " + formatString + " contans too many regular expressions - found " + mp.size() + ", maximum is 4");
		}
	}

	private String estimateOutputValue(StringFormat inputRegexp, String defaultOutputValue) {
		return estimateOutputValue(inputRegexp.getPattern().toString(), defaultOutputValue);
	}

	private String estimateOutputValue(String inputRegexp, String defaultOutputValue) {
		Matcher m = ESTIMATE_OUTPUT.matcher(inputRegexp);
		if (m.matches()) {
			return m.group(1);
		} else {
			return defaultOutputValue;
		}
	}

	@Override
	public boolean parseBoolean(CharSequence seq) throws ParseBooleanException {
		if (trueStringFormat.matches(seq)) {
			return true;
		} else if (falseStringFormat.matches(seq)) {
			return false;
		} else {
			throw new ParseBooleanException("Error parse '" + seq + "'" + " - value don't match trueFormat " + trueStringFormat + " nor falseFormat " + falseStringFormat);
		}
	}

	@Override
	public String formatBoolean(boolean value) {
		if (value) {
			return trueOutputString;
		} else {
			return falseOutputString;
		}
	}

	@Override
	public String toString() {
		return "[CloverBooleanFormatter]" + trueStringFormat + "/" + falseStringFormat;
	}

}

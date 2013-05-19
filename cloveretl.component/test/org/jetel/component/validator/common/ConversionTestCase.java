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
package org.jetel.component.validator.common;

import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.component.validator.rules.ConversionValidationRule;

/**
 * Helper methods for testing rules extending {@link ConversionValidationRule}
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.5.2013
 */
public class ConversionTestCase extends ValidatorTestCase {

	/**
	 * Sets number format
	 * @param format Number format to set
	 * @param rule Rule on which the setting should be done
	 * @return Modified rule (the same)
	 */
	protected ConversionValidationRule nf(String format, ConversionValidationRule rule) {
		rule.getLanguageSettings(0).getNumberFormat().setValue(format);
		return rule;
	}
	
	/**
	 * Sets date format
	 * @param format Date format to set
	 * @param rule Rule on which the setting should be done
	 * @return Modified rule (the same)
	 */
	protected ConversionValidationRule df(String format, ConversionValidationRule rule) {
		rule.getLanguageSettings(0).getDateFormat().setValue(format);
		return rule;
	}
	
	/**
	 * Sets timezone
	 * @param format Timezone to set
	 * @param rule Rule on which the setting should be done
	 * @return Modified rule (the same)
	 */
	protected ConversionValidationRule tz(String timezone, ConversionValidationRule rule) {
		rule.getLanguageSettings(0).getTimezone().setValue(timezone);
		return rule;
	}
	
	/**
	 * Sets locale
	 * @param format Number format to set
	 * @param rule Rule on which the setting should be done
	 * @return Modified rule (the same)
	 */
	protected ConversionValidationRule lo(String locale, ConversionValidationRule rule) {
		rule.getLanguageSettings(0).getLocale().setValue(locale);
		return rule;
	}
	
	/**
	 * Sets type used for comparison/interval/... check.
	 * @param type s - string, da - date, l - long, n - number, d - decimal
	 * @param rule Rule on which the setting should be done
	 */
	protected ConversionValidationRule inType(String type, ConversionValidationRule rule) {
		ConversionValidationRule.METADATA_TYPES t = null;
		if(type.equals("s")) {
			t = ConversionValidationRule.METADATA_TYPES.STRING;
		} else if(type.equals("da")) {
			t = ConversionValidationRule.METADATA_TYPES.DATE;
		} else if(type.equals("l")) {
			t = ConversionValidationRule.METADATA_TYPES.LONG;
		} else if(type.equals("n")) {
			t = ConversionValidationRule.METADATA_TYPES.NUMBER;
		} else if(type.equals("d")) {
			t = ConversionValidationRule.METADATA_TYPES.DECIMAL;
		}
		rule.getUseType().setValue(t);
		return rule;
	}
}

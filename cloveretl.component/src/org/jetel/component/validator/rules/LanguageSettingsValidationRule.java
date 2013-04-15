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
package org.jetel.component.validator.rules;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.metadata.DataFieldFormatType;
import org.joda.time.format.DateTimeFormat;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 12.3.2013
 */
@XmlType(propOrder={"languageSettings"})
public abstract class LanguageSettingsValidationRule extends AbstractValidationRule {
	
	@XmlElementWrapper(name="languageSettings",required=false)
	@XmlElementRef
	private List<LanguageSetting> languageSettings = new ArrayList<LanguageSetting>();

	public List<LanguageSetting> getLanguageSettings() {
		return languageSettings;
	}
	
	public void addLanguageSetting(LanguageSetting value) {
		languageSettings.add(value);
	}
	
	public LanguageSetting getLanguageSettings(int index) {
		try {
			return languageSettings.get(index);
		} catch (Exception ex) {
			return null;
		}
	}
	
	/**
	 * Log language settings
	 */
	public void logLanguageSettings() {
		int i = 0;
		for(LanguageSetting temp : languageSettings) {
			logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' has language setting #" + i++ + ":\n" + temp);	
		}
	}
	
	protected boolean isDateFormatReady(String resolvedInput, StringEnumValidationParamNode originalParamNode, ReadynessErrorAcumulator accumulator) {
		DataFieldFormatType formatType = DataFieldFormatType.getFormatType(resolvedInput);
		if(formatType == DataFieldFormatType.JAVA || formatType == null) {
			try {
				new SimpleDateFormat(formatType.getFormat(resolvedInput));
			} catch (Exception ex) {
				accumulator.addError(originalParamNode, this, "Format mask is not in java format syntax.");
				return false;
			}
		} else if(formatType == DataFieldFormatType.JODA) {
			try {
				DateTimeFormat.forPattern(formatType.getFormat(resolvedInput));
			} catch (Exception ex) {
				accumulator.addError(originalParamNode, this, "Format mask is not in joda format syntax.");
				return false;
			}
		} else {
			accumulator.addError(originalParamNode, this, "Unknown format mask prefix.");
			return false;
		}
		return true;
	}
	
	protected boolean isLocaleReady(String resolvedInput, StringEnumValidationParamNode originalParamNode, ReadynessErrorAcumulator accumulator) {
		if(resolvedInput.isEmpty()) {
			accumulator.addError(originalParamNode, this, "Locale is empty.");
			return false;
		}
		return true;
	}
	
	protected boolean isTimezoneReady(String resolvedInput, StringEnumValidationParamNode originalParamNode, ReadynessErrorAcumulator accumulator) {
		if(resolvedInput.isEmpty()) {
			accumulator.addError(originalParamNode, this, "Timezone is empty.");
			return false;
		}
		return true;
	}
	
	protected boolean isNumberFormatReady(String resolvedInput, StringEnumValidationParamNode originalParamNode, ReadynessErrorAcumulator accumulator) {
		try {
			DecimalFormat numberFormat = (DecimalFormat) DecimalFormat.getInstance();
			if(!resolvedInput.isEmpty()) {
				numberFormat.applyLocalizedPattern(resolvedInput);
			}
		} catch (Exception ex) {
			accumulator.addError(originalParamNode, this, "Format mask is not correct.");
			return false;
		}
		return true;
	}
}

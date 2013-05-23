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
 * <p>Abstract class providing capability to store multiple {@link LanguageSetting} from child class.</p>
 * 
 * <p>Most usages will be through {@link ConversionValidationRule} and {@link StringValidationRule}.</p>
 * 
 * For using directly:
 * <ul> 
 *  <li><code>addLanguageSetting(new LanguageSetting());</code> somewhere in constructor</li>
 *  <li>in {@link #initialize()} initialize and hide unused items of language settings, for example:<br />
 *  <code>
 *  	LanguageSetting languageSetting = getLanguageSettings(0);
 *		languageSetting.initialize();
 *		languageSetting.getNumberFormat().setHidden(true); 
 *	</code>
 *	</li>
 *  <li>Log it <code>logLanguageSettings();</code></li>
 *  <li>Take inherited language settings into account, for example:<br />
 *  <code>LanguageSetting computedLS = LanguageSetting.hierarchicMerge(getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0), parentLanguageSetting);</code>
 *  </li>
 *  <li>Use values from <code>computedLS</code></li>
 * </ul>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 12.3.2013
 * @see DateValidationRule
 * @see NumberValidationRule
 * @see ConversionValidationRule
 * @see StringValidationRule
 */
@XmlType(propOrder={"languageSettings"})
public abstract class LanguageSettingsValidationRule extends AbstractValidationRule {
	
	@XmlElementWrapper(name="languageSettings",required=false)
	@XmlElementRef
	private List<LanguageSetting> languageSettings = new ArrayList<LanguageSetting>();

	/**
	 * Returns all stored language settings
	 * @return All language settings
	 */
	public List<LanguageSetting> getLanguageSettings() {
		return languageSettings;
	}
	
	/**
	 * Store new language setting
	 * @param value New language setting
	 */
	public void addLanguageSetting(LanguageSetting value) {
		languageSettings.add(value);
	}
	
	/**
	 * Returns language setting with given id
	 * @param index
	 * @return language setting
	 */
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
	
	/**
	 * Checks whether given date formatting mask is correct.
	 * @param resolvedInput Input where all params are resolved
	 * @param originalParamNode Param node with formatting mask (to raise error when needed)
	 * @param accumulator Accumulator of errors
	 * @return True if correct, false otherwise
	 */
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
	
	/**
	 * Checks whether given locale is correct.
	 * @param resolvedInput Input where all params are resolved
	 * @param originalParamNode Param node with locale (to raise error when needed)
	 * @param accumulator Accumulator of errors
	 * @return True if correct, false otherwise.
	 */
	protected boolean isLocaleReady(String resolvedInput, StringEnumValidationParamNode originalParamNode, ReadynessErrorAcumulator accumulator) {
		if(resolvedInput.isEmpty()) {
			accumulator.addError(originalParamNode, this, "Locale is empty.");
			return false;
		}
		return true;
	}
	
	/**
	 * Checks whether given timezone is correct.
	 * @param resolvedInput Input where all params are resolved
	 * @param originalParamNode Param node with timezone (to raise error when needed)
	 * @param accumulator Accumulator of errors
	 * @return True if correct, false otherwise
	 */
	protected boolean isTimezoneReady(String resolvedInput, StringEnumValidationParamNode originalParamNode, ReadynessErrorAcumulator accumulator) {
		if(resolvedInput.isEmpty()) {
			accumulator.addError(originalParamNode, this, "Timezone is empty.");
			return false;
		}
		return true;
	}
	
	/**
	 * Checks whether given number formatting mask is correct.
	 * @param resolvedInput Input where all params are resolved.
	 * @param originalParamNode Param node with formatting mask (to raise error when needed).
	 * @param accumulator Accumulator of errors
	 * @return True if correct, false otherwise
	 */
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

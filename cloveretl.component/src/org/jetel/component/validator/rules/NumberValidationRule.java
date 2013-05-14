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
import java.text.ParsePosition;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * <p>Rule for checking that incoming string field is number according to specific format.</p>
 * 
 * Available settings:
 * <ul>
 * 	<li>Trim input. If input should be trimmed before parsing.</p>
 * </ul>
 * 
 * <p>Formatting mask and locale is inherited from {@link LanguageSettingsValidationRule}.</p>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.3.2013
 * @see LanguageSettingsValidationRule
 */
@XmlRootElement(name="number")
@XmlType(propOrder={"trimInput"})
public class NumberValidationRule extends LanguageSettingsValidationRule {
	
	public static final int ERROR_LEFTOVERS = 401;	/** Input after parsing was not empty. */
	public static final int ERROR_PARSING = 402;	/** Parsing failed. */
	public static final int ERROR_STRING = 403;		/** Input field was not string. */
	
	private static final int LANGUAGE_SETTING_ACCESSOR_0 = 0;
	
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);

	public NumberValidationRule() {
		addLanguageSetting(new LanguageSetting());
	}
	
	public List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		trimInput.setName("Trim input");
		trimInput.setTooltip("Trim input before validation.");
		params.add(trimInput);
		
		LanguageSetting languageSetting = getLanguageSettings(0);
		languageSetting.initialize();
		languageSetting.getDateFormat().setHidden(true);
		languageSetting.getTimezone().setHidden(true);
		return params;
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		setPropertyRefResolver(graphWrapper);
		logParams(StringUtils.mapToString(getProcessedParams(record.getMetadata(), graphWrapper), "=", "\n"));
		logParentLangaugeSetting();
		logLanguageSettings();
		
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0), parentLanguageSetting);
		
		String resolvedTarget = resolve(target.getValue());
		String resolvedFormat = resolve(computedLS.getNumberFormat().getValue());
		String resolvedLocale = resolve(computedLS.getLocale().getValue());
		
		DataField field = record.getField(target.getValue());
		// Null values are valid by definition
		if(field.isNull()) {
			logSuccess("Field '" + resolvedTarget + "' is null.");
			return State.VALID;
		}
		if(field.getMetadata().getDataType() != DataFieldType.STRING) {
			logError("Field '" + resolvedTarget + "' is not a string.");
			raiseError(ea, ERROR_STRING, "The target field is not a string.", graphWrapper.getNodePath(this), resolvedTarget, field.getValue().toString());
			return State.INVALID;
		}
		
		String tempString = field.toString();
		if(trimInput.getValue()) {
			tempString = tempString.trim();
		}
		
		Locale realLocale = ValidatorUtils.localeFromString(resolvedLocale);
		try {
			DecimalFormat numberFormat = (DecimalFormat) DecimalFormat.getInstance(realLocale);
			// Special handling with two named formatting masks
			if(computedLS.getNumberFormat().getValue().equals(CommonFormats.INTEGER)) {
				numberFormat.applyPattern("#");
				numberFormat.setParseIntegerOnly(true);
			} else if(computedLS.getNumberFormat().getValue().equals(CommonFormats.NUMBER)) {
					numberFormat.applyPattern("#");
			} else if(!computedLS.getNumberFormat().getValue().isEmpty()) {
				numberFormat.applyPattern(resolvedFormat);
			}
			ParsePosition pos = new ParsePosition(0);
			Number parsedNumber = numberFormat.parse(tempString, pos);
			if(parsedNumber == null || pos.getIndex() != tempString.length()) {
				logError("Field '" + resolvedTarget + "' with value '" + tempString + "' contains leftovers after parsed value.");
				raiseError(ea, ERROR_PARSING, "The target filed could not be parsed.", graphWrapper.getNodePath(this), resolvedTarget, tempString);
				return State.INVALID;
			}
			logSuccess("Field '" + resolvedTarget + "' parsed as '" + parsedNumber + "'");
			return State.VALID;
		} catch (Exception ex) {
			logError("Field '" + resolvedTarget + "' with value '" + tempString + "' could not be parsed.");
			raiseError(ea, ERROR_PARSING, "The target field could not be parsed.", graphWrapper.getNodePath(this), resolvedTarget, tempString);
			return State.INVALID;
		}
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		setPropertyRefResolver(graphWrapper);
		LanguageSetting originalLS = getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0);
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(originalLS, parentLanguageSetting);
		
		String resolvedTarget = resolve(target.getValue());
		String resolvedFormat = resolve(computedLS.getNumberFormat().getValue());
		String resolvedLocale = resolve(computedLS.getLocale().getValue());
		
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		} else {
			if(inputMetadata.getField(resolvedTarget) != null && inputMetadata.getField(resolvedTarget).getDataType() != DataFieldType.STRING) {
				accumulator.addError(target, this, "Target field is not string.");
				state = false;	
			}
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		
		state &= isLocaleReady(resolvedLocale, originalLS.getLocale(), accumulator);
		state &= isNumberFormatReady(resolvedFormat, originalLS.getNumberFormat(), accumulator);

		return state;
	}

	@Override
	public String getCommonName() {
		return "Number";
	}

	@Override
	public String getCommonDescription() {
		return "Checks whether chosen field is a number in provided format.";
	}
}

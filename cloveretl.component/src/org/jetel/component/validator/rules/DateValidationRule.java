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

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * <p>Rule for checking that incoming string field is date according to specific format.</p>
 * 
 * Available settings:
 * <ul>
 * 	<li>Trim input. If input should be trimmed before parsing.</p>
 * </ul>
 * 
 * <p>Formatting mask, timezone, locale is inherited from {@link LanguageSettingsValidationRule}.</p>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.3.2013
 * @see LanguageSettingsValidationRule
 */
@XmlRootElement(name="date")
@XmlType(propOrder={"trimInput"})
public class DateValidationRule extends LanguageSettingsValidationRule {
	
	public static final int ERROR_DOUBLE_CHECK = 301;	/** Input after parsing was not empty */
	public static final int ERROR_PARSING = 302;		/** Parsing unsuccessful */
	public static final int ERROR_STRING = 303;			/** Input was not string */
	
	private static final int LANGUAGE_SETTING_ACCESSOR_0 = 0;
	
	@XmlElement(name="trimInput",required=false)
	private BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	
	public DateValidationRule() {
		addLanguageSetting(new LanguageSetting());
	}
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		trimInput.setName("Trim input");
		trimInput.setTooltip("Trim input before validation.");
		
		LanguageSetting languageSetting = getLanguageSettings(0);
		languageSetting.initialize();
		languageSetting.getNumberFormat().setHidden(true); // No need for number format
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(trimInput);
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
		String resolvedFormat = resolve(computedLS.getDateFormat().getValue());
		String resolvedLocale = resolve(computedLS.getLocale().getValue());
		String resolvedTimezone = resolve(computedLS.getTimezone().getValue());
		
		DataField field = record.getField(resolvedTarget);
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
		DataFieldFormatType formatType = DataFieldFormatType.getFormatType(resolvedFormat);
	
		Locale realLocale = ValidatorUtils.localeFromString(resolvedLocale);
		if(formatType == DataFieldFormatType.JAVA || formatType == null) {
			try {
				SimpleDateFormat dateFormat;
				if (formatType == null) {
					dateFormat = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT, realLocale);
				} else {
					dateFormat = new SimpleDateFormat(formatType.getFormat(resolvedFormat), realLocale);
				}
				dateFormat.setTimeZone(TimeZone.getTimeZone(resolvedTimezone));
				
				Date parsedDate = dateFormat.parse(tempString);
				if(!dateFormat.format(parsedDate).equals(tempString.trim())) {
					logError("Field '" + resolvedTarget + "' parsed as '" + parsedDate.toString() + "' is not a date with given settings (double check failed).");
					raiseError(ea, ERROR_DOUBLE_CHECK, "The target field is not correct date, double check failed.", graphWrapper.getNodePath(this), resolvedTarget, tempString);
					return State.INVALID;
				}
				logSuccess("Field '" + resolvedTarget + "' parsed as '" + parsedDate.toString() + "' is date with given settings.");
				return State.VALID;
			} catch (Exception ex) {
				logError("Field '" + resolvedTarget + "' with value '" + tempString + "' is not a date with given settings.");
				raiseError(ea, ERROR_PARSING, "The target field could not be parsed.", graphWrapper.getNodePath(this), resolvedTarget, tempString);
				return State.INVALID;	
			}
		} else {
			try {
				DateTimeFormatter formatter = DateTimeFormat.forPattern(formatType.getFormat(resolvedFormat));
				formatter = formatter.withLocale(realLocale);
				formatter = formatter.withZone(DateTimeZone.forID(resolvedTimezone));
				DateTime parsedDate = formatter.parseDateTime(tempString);
				if(!parsedDate.toString(formatter).equals(tempString.trim())) {
					logError("Field '" + resolvedTarget + "' parsed as '" + parsedDate.toString() + "' is not a date with given settings (double check failed).");
					raiseError(ea, ERROR_DOUBLE_CHECK, "The target field is not correct date, double check failed.", graphWrapper.getNodePath(this), resolvedTarget, tempString);
					return State.INVALID;
				}
				logSuccess("Field '" + resolvedTarget + "' parsed as '" + parsedDate.toString() + "' is date with given settings.");
				return State.VALID;
			} catch (Exception ex) {
				logError("Field '" + resolvedTarget + "' with value '" + tempString + "' is not a date with given settings.");
				raiseError(ea, ERROR_PARSING, "The target field could not be parsed.", graphWrapper.getNodePath(this), resolvedTarget, tempString);
				return State.INVALID;
			}
		}
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		setPropertyRefResolver(graphWrapper);
		boolean state = true;
		LanguageSetting originalLS = getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0);
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(originalLS, parentLanguageSetting);
		String resolvedTarget = resolve(target.getValue());
		String resolvedLocale = resolve(computedLS.getLocale().getValue());
		String resolvedTimezone = resolve(computedLS.getTimezone().getValue());
		String resolvedFormat = resolve(computedLS.getDateFormat().getValue());
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
		state &= isTimezoneReady(resolvedTimezone, originalLS.getTimezone(), accumulator);
		state &= isDateFormatReady(resolvedFormat, originalLS.getDateFormat(), accumulator);
		return state;
	}

	@Override
	public String getCommonName() {
		return "Date";
	}

	@Override
	public String getCommonDescription() {
		return "Checks whether chosen field is a date in provided format.";
	}

}

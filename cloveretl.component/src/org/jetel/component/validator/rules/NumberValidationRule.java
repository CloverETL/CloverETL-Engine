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

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.3.2013
 */
@XmlRootElement(name="number")
@XmlType(propOrder={"trimInput", "format", "locale"})
public class NumberValidationRule extends AbstractValidationRule {
	
	public static final int ERROR_LEFTOVERS = 401;
	public static final int ERROR_PARSING = 402;
	
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);

	@XmlElement(name="format",required=true)
	private StringEnumValidationParamNode format = new StringEnumValidationParamNode(CommonFormats.defaultNumber);	
	
	@XmlElement(name="locale",required=true)
	private StringEnumValidationParamNode locale = new StringEnumValidationParamNode(Defaults.DEFAULT_LOCALE);
	
	public List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		trimInput.setName("Trim input");
		trimInput.setTooltip("Trim input before validation.");
		params.add(trimInput);
		format.setName("Format mask");
		format.setPlaceholder("Number format, for syntax see documentation.");
		format.setOptions(CommonFormats.numbers);
		params.add(format);
		locale.setName("Locale");
		locale.setOptions(CommonFormats.locales);
		locale.setTooltip("Locale code of record field");
		params.add(locale);
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
		
		String resolvedTarget = resolve(target.getValue());
		String resolvedFormat = resolve(format.getValue());
		String resolvedLocale = resolve(locale.getValue());
		
		DataField field = record.getField(target.getValue());
		if(field.isNull()) {
			logSuccess("Field '" + resolvedTarget + "' is null.");
			return State.VALID;
		}
		if(field.getMetadata().getDataType() != DataFieldType.STRING) {
			logError("Field '" + resolvedTarget + "' is not a string.");
			return State.INVALID;
		}
		
		String tempString = field.toString();
		if(trimInput.getValue()) {
			tempString = tempString.trim();
		}
		
		Locale realLocale = ValidatorUtils.localeFromString(resolvedLocale);
		try {
			DecimalFormat numberFormat = (DecimalFormat) DecimalFormat.getInstance(realLocale);
			if(format.getValue().equals(CommonFormats.defaultNumber)) {
				numberFormat.applyPattern("#");
				numberFormat.setParseIntegerOnly(true);
			} else if(!format.getValue().isEmpty()) {
				//numberFormat.applyLocalizedPattern(format.getValue());
				numberFormat.applyPattern(resolvedFormat);
			}
			ParsePosition pos = new ParsePosition(0);
//			numberFormat.setMinimumFractionDigits(0);
//			numberFormat.setMaximumFractionDigits(0);
//			numberFormat.setMaximumIntegerDigits(0);
			Number parsedNumber = numberFormat.parse(tempString, pos);
			if(parsedNumber == null || pos.getIndex() != tempString.length()) {
				logError("Field '" + resolvedTarget + "' with value '" + tempString + "' contains leftovers after parsed value.");
				raiseError(ea, ERROR_PARSING, "The target field contains leftovers after parsed value.", resolvedTarget, tempString);
				return State.INVALID;
			}
			logSuccess("Field '" + resolvedTarget + "' parsed as '" + parsedNumber + "'");
			return State.VALID;
		} catch (Exception ex) {
			logError("Field '" + resolvedTarget + "' with value '" + tempString + "' could not be parsed.");
			raiseError(ea, ERROR_PARSING, "The target field could not be parsed.", resolvedTarget, tempString);
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
		String resolvedTarget = resolve(target.getValue());
		String resolvedFormat = resolve(format.getValue());
		String resolvedLocale = resolve(locale.getValue());
		
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
		try {
			DecimalFormat numberFormat = (DecimalFormat) DecimalFormat.getInstance();
			if(!resolvedFormat.isEmpty()) {
				numberFormat.applyLocalizedPattern(resolvedFormat);
			}
		} catch (Exception ex) {
			accumulator.addError(format, this, "Format mask is not correct.");
			state = false;
		}
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
	
	public StringEnumValidationParamNode getFormat() {
		return format;
	}

	public StringEnumValidationParamNode getLocale() {
		return locale;
	}
}

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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.formatter.NumericFormatter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.3.2013
 */
@XmlRootElement(name="number")
@XmlType(propOrder={"format", "locale"})
public class NumberValidationRule extends StringValidationRule {

	@XmlElement(name="format",required=true)
	private StringEnumValidationParamNode format = new StringEnumValidationParamNode(CommonFormats.defaultNumber);	
	
	@XmlElement(name="locale",required=true)
	private StringEnumValidationParamNode locale = new StringEnumValidationParamNode(Defaults.DEFAULT_LOCALE);
	
	public List<ValidationParamNode> initialize() {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		format.setName("Format mask");
		format.setPlaceholder("Number format, for syntax see documentation.");
		format.setOptions(CommonFormats.numbers);
		params.add(format);
		locale.setName("Locale");
		locale.setOptions(CommonFormats.locales);
		locale.setTooltip("Locale code of record field");
		params.add(locale);
		params.addAll(super.initialize());
		return params;
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		logger.trace("Validation rule: " + this.getName() + "\n"
					+ "Target field: " + target.getValue() + "\n"
					+ "Format mask: " + format.getValue() + "\n"
					+ "Locale: " + locale.getValue() + "\n"
					+ "Trim input: " + trimInput.getValue());
		
		DataField field = record.getField(target.getValue());
		if(field.isNull()) {
			logger.trace("Validation rule: " + getName() + "  on null is " + State.VALID);
			return State.VALID;
		}
		
		Locale realLocale = ValidatorUtils.localeFromString(locale.getValue());
		String tempString = prepareInput(record.getField(target.getValue()));
		try {
			DecimalFormat numberFormat = (DecimalFormat) DecimalFormat.getInstance(realLocale);
			if(format.getValue().equals(CommonFormats.defaultNumber)) {
				numberFormat.applyLocalizedPattern("#");
				numberFormat.setParseIntegerOnly(true);
			} else if(!format.getValue().isEmpty()) {
				//numberFormat.applyLocalizedPattern(format.getValue());
				numberFormat.applyPattern(format.getValue());
			}
			ParsePosition pos = new ParsePosition(0);
//			numberFormat.setMinimumFractionDigits(0);
//			numberFormat.setMaximumFractionDigits(0);
//			numberFormat.setMaximumIntegerDigits(0);
			Number parsedNumber = numberFormat.parse(tempString, pos);
			System.err.println(parsedNumber);
			if(parsedNumber == null || pos.getIndex() != tempString.length()) {
				logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' parsed as '" + parsedNumber + "' is " + State.INVALID);
				return State.INVALID;
			}
			logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' parsed as '" + parsedNumber + "' is " + State.VALID);
			return State.VALID;
		} catch (Exception ex) {
			logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' could not parse, therefore is " + State.INVALID);
			return State.INVALID;
		}
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		if(target.getValue().isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(!ValidatorUtils.isValidField(target.getValue(), inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		try {
			String localeString = (inputMetadata.getLocaleStr() == null) ? Defaults.DEFAULT_LOCALE : inputMetadata.getLocaleStr(); 
			Locale locale = new Locale(localeString);
			DecimalFormat numberFormat = (DecimalFormat) DecimalFormat.getInstance(locale);
			if(!format.getValue().isEmpty()) {
				numberFormat.applyLocalizedPattern(format.getValue());
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

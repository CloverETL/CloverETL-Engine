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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataRecordMetadata;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.3.2013
 */
@XmlRootElement(name="date")
@XmlType(propOrder={"format", "strict"})
public class DateValidationRule extends StringValidationRule {
	
	/**
	 * @see (DateFormatAttributeType)
	 */
	private final String[] commonFormats = {
		"yyyy-MM-dd HH:mm:ss",
		"yyyy-MM-dd",
		"HH:mm:ss",
		"dd.MM.yy",
		"dd/MM/yy",
		"dd.MM.yyyy",
		"MM.dd.yyyy",
		"yyyy-MM-dd hh:mm:ss 'text'",
		"yyyy.MM.dd HH:mm:ss.SSS z",
		"EEE, MMM d, yy",
		"joda:yyyy-MM-dd HH:mm:ss",
		"joda:yyyy-MM-dd",
		"joda:HH:mm:ss",
		"joda:dd.MM.yy",
		"joda:dd/MM/yy",
		"joda:dd.MM.yyyy",
		"joda:MM.dd.yyyy",
		"joda:yyyy-MM-dd hh:mm:ss 'text'",
		"joda:EEE, MMM d, yy",
	};

	@XmlElement(name="format",required=true)
	private StringEnumValidationParamNode format = new StringEnumValidationParamNode(commonFormats[0]);	
	
	@XmlElement(name="strict",required=false)
	private BooleanValidationParamNode strict = new BooleanValidationParamNode(false);
	
	public List<ValidationParamNode> initialize() {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		format.setName("Format mask");
		format.setPlaceholder("Date format, for syntax see documentation");
		format.setOptions(commonFormats);
		params.add(format);
		strict.setName("Strict mode");
		params.add(strict);
		params.addAll(super.initialize());
		return params;
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		logger.trace("Validation rule: " + this.getName() + "\n"
					+ "Target field: " + target.getValue() + "\n"
					+ "Format mask: " + format.getValue() + "\n"
					+ "Strict mode: " + strict.getValue() + "\n"
					+ "Trim input: " + trimInput.getValue());
		
		// TODO: add timezone support
		String localeString = (record.getMetadata().getLocaleStr() == null) ? Defaults.DEFAULT_LOCALE : record.getMetadata().getLocaleStr(); 
		Locale locale = new Locale(localeString);
		
		String tempString = prepareInput(record.getField(target.getValue()));
		DataFieldFormatType formatType = DataFieldFormatType.getFormatType(format.getValue());
	
		if(formatType == DataFieldFormatType.JAVA || formatType == null) {
			try {
				SimpleDateFormat dateFormat;
				if (formatType == null) {
					dateFormat = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT, locale);
				} else {
					 dateFormat = new SimpleDateFormat(formatType.getFormat(format.getValue()), locale);
				}
				
				Date parsedDate = dateFormat.parse(tempString);
				if(strict.getValue() && !dateFormat.format(parsedDate).equals(tempString.trim())) {
					logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.INVALID);
					return State.INVALID;
				}
				logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.VALID);
				return State.VALID;
			} catch (Exception ex) {
				logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.INVALID);
				return State.INVALID;	
			}
		} else {
			try {
				DateTimeFormatter formatter = DateTimeFormat.forPattern(formatType.getFormat(format.getValue()));
				formatter = formatter.withLocale(locale);
				DateTime parsedDate = formatter.parseDateTime(tempString);
				if(strict.getValue() && !parsedDate.toString(formatter).equals(tempString.trim())) {
					logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.INVALID);
					return State.INVALID;
				}
				logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.VALID);
				return State.VALID;
			} catch (Exception ex) {
				logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.INVALID);
				return State.INVALID;
			}
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
		DataFieldFormatType formatType = DataFieldFormatType.getFormatType(format.getValue());
		if(formatType == DataFieldFormatType.JAVA || formatType == null) {
			try {
				new SimpleDateFormat(formatType.getFormat(format.getValue()));
			} catch (Exception ex) {
				accumulator.addError(format, this, "Format mask is not in java format syntax.");
				state = false;	
			}
		} else if(formatType == DataFieldFormatType.JODA) {
			try {
				DateTimeFormat.forPattern(formatType.getFormat(format.getValue()));
			} catch (Exception ex) {
				accumulator.addError(format, this, "Format mask is not in joda format syntax.");
				state = false;
			}
		} else {
			accumulator.addError(format, this, "Unknown format mask prefix.");
			state = false;
		}
		return state;
	}

	@Override
	public String getCommonName() {
		return "Validate date";
	}

	@Override
	public String getCommonDescription() {
		return "Checks whether chosen field is a date in provided format.";
	}
	
	public StringEnumValidationParamNode getFormat() {
		return format;
	}

	public BooleanValidationParamNode getStrict() {
		return strict;
	}
}

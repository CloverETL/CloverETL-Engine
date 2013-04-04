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
import java.util.TimeZone;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.BooleanDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.DecimalDataField;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlType(propOrder={"format", "locale", "timezone", "trimInput"})
public abstract class StringValidationRule extends AbstractValidationRule{
	
	@XmlElement(name="format", required=false)
	protected StringEnumValidationParamNode format = new StringEnumValidationParamNode();
	
	@XmlElement(name="locale", required=false)
	// FIXME: fixed from GUI or global?
	protected StringEnumValidationParamNode locale = new StringEnumValidationParamNode("en.US");
	
	@XmlElement(name="timezone", required=false)
	protected StringEnumValidationParamNode timezone = new StringEnumValidationParamNode("UTC");
	
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	
	public DataFieldMetadata safeGetFieldMetadata(DataRecordMetadata inputMetadata, String name) {
		String[] allFieldNames = inputMetadata.getFieldNamesArray();
		for(String temp : allFieldNames) {
			if(temp.equals(name)) {
				return inputMetadata.getField(name);
			}
		}
		return null;
	}

	public List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		final DataRecordMetadata inputMetadata = inMetadata;
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		format.setName("Format mask");
		format.setOptions(CommonFormats.all);
		format.setTooltip("Format mask to format input field before validation.\n");
		format.setPlaceholder("Number/date format, for syntax see documentation.");
		params.add(format);
		format.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() != DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		locale.setName("Locale");
		locale.setOptions(CommonFormats.locales);
		locale.setTooltip("Locale code of record field");
		params.add(locale);
		locale.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() != DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		timezone.setName("Timezone");
		timezone.setOptions(CommonFormats.timezones);
		timezone.setTooltip("Timezone code of record field");
		params.add(timezone);
		timezone.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() == DataFieldType.DATE) {
					return true;
				}
				return false;
			}
		});
		trimInput.setName("Trim input");
		params.add(trimInput);
		trimInput.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() == DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		return params;
	}
	
	protected String prepareInput(DataRecord record, String name) {
		String resolvedFormat = resolve(format.getValue());
		String resolvedLocale = resolve(locale.getValue());
		String resolvedTimezone = resolve(timezone.getValue());
		
		DataFieldMetadata fieldMetadata = safeGetFieldMetadata(record.getMetadata(), name);
		if(fieldMetadata == null) {
			throw new IllegalArgumentException("Unknown field.");
		}
		DataField field = record.getField(name);
		if(field.isNull()) {
			return "";
		}
		if(fieldMetadata.getDataType() == DataFieldType.BOOLEAN) {
			if(((BooleanDataField) field).getValue()) {
				return "True";
			} else {
				return "False";
			}
		} else if(fieldMetadata.getDataType() == DataFieldType.DATE
				|| fieldMetadata.getDataType() == DataFieldType.DATETIME) {
			String formatString;
			if(resolvedFormat.isEmpty()) {
				formatString = Defaults.DEFAULT_DATETIME_FORMAT;
			} else {
				formatString = resolvedFormat;
			}
			SimpleDateFormat dateFormat = new SimpleDateFormat(formatString, ValidatorUtils.localeFromString(resolvedLocale));
			dateFormat.setTimeZone(TimeZone.getTimeZone(resolvedTimezone));
			return dateFormat.format(((DateDataField) field).getValue());
		} else if(fieldMetadata.getDataType() == DataFieldType.DECIMAL 
				|| fieldMetadata.getDataType() == DataFieldType.NUMBER
				|| fieldMetadata.getDataType() == DataFieldType.LONG
				|| fieldMetadata.getDataType() == DataFieldType.INTEGER) {
			if(resolvedFormat.isEmpty()) {
				return field.getValue().toString();
			}
			DecimalFormat decimalFormat = (DecimalFormat) DecimalFormat.getInstance(ValidatorUtils.localeFromString(resolvedLocale));
			if(format.getValue().equals(CommonFormats.INTEGER)) {
				decimalFormat.applyPattern("#");
				decimalFormat.setGroupingUsed(false); // Suppress grouping of thousand by default
			} else {
				decimalFormat.applyPattern(resolvedFormat);
			}
			if(fieldMetadata.getDataType() == DataFieldType.DECIMAL) {
				return decimalFormat.format(((DecimalDataField) field).getBigDecimal());
			} else {
				return decimalFormat.format(field.getValue());
			}
		} else if(fieldMetadata.getDataType() == DataFieldType.BYTE
				|| fieldMetadata.getDataType() == DataFieldType.CBYTE) {
			// TODO: ???
			return field.getValue().toString();
		} else if(fieldMetadata.getDataType() == DataFieldType.STRING) {
			String temp = ((StringDataField) field).getValue().toString();
			if(trimInput.getValue()) {
				temp = temp.trim();
			}
			return temp;
		} else {
			return field.getValue().toString();
		}
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		setPropertyRefResolver(graphWrapper);
		boolean state = true;
		String resolvedLocale = resolve(locale.getValue());
		String resolvedTimezone = resolve(timezone.getValue());
		DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
		if(fieldMetadata != null && fieldMetadata.getDataType() != DataFieldType.STRING) {
			if(resolvedLocale.isEmpty()) {
				accumulator.addError(locale, this, "Empty locale.");
				state &= false;
			}
			if(resolvedTimezone.isEmpty()) {
				accumulator.addError(timezone, this, "Empty timezone.");
				state &= false;
			}
		}
		return state;
	}

	/**
	 * @return the trimInput
	 */
	public BooleanValidationParamNode getTrimInput() {
		return trimInput;
	}
	/**
	 * @return the format
	 */
	public StringEnumValidationParamNode getFormat() {
		return format;
	}
	/**
	 * @return the locale
	 */
	public StringEnumValidationParamNode getLocale() {
		return locale;
	}
	/**
	 * @return the timezone
	 */
	public StringEnumValidationParamNode getTimezone() {
		return timezone;
	}
}

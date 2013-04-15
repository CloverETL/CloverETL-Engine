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
import org.jetel.component.validator.params.LanguageSetting;
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
@XmlType(propOrder={"trimInput"})
public abstract class StringValidationRule extends LanguageSettingsValidationRule {
	
	protected static final int LANGUAGE_SETTING_ACCESSOR_0 = 0;
	
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	
	public StringValidationRule() {
		addLanguageSetting(new LanguageSetting());
	}
	
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
		
		LanguageSetting languageSetting = getLanguageSettings(0);
		languageSetting.initialize();
		
		languageSetting.getNumberFormat().setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
				if(fieldMetadata != null && (fieldMetadata.getDataType() == DataFieldType.INTEGER || fieldMetadata.getDataType() == DataFieldType.LONG || fieldMetadata.getDataType() == DataFieldType.NUMBER || fieldMetadata.getDataType() == DataFieldType.DECIMAL)) {
					return true;
				}
				return false;
			}
		});
		
		languageSetting.getDateFormat().setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
				if(fieldMetadata != null && (fieldMetadata.getDataType() == DataFieldType.DATE)) {
					return true;
				}
				return false;
			}
		});
		
		languageSetting.getLocale().setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() != DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		languageSetting.getTimezone().setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() == DataFieldType.DATE) {
					return true;
				}
				return false;
			}
		});
		
		return params;
	}
	
	protected String prepareInput(DataRecord record, String name) {
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0), parentLanguageSetting);
		
		DataFieldMetadata fieldMetadata = safeGetFieldMetadata(record.getMetadata(), name);
		if(fieldMetadata == null) {
			throw new IllegalArgumentException("Unknown field.");
		}
		
		String resolvedFormat;
		if(fieldMetadata.getDataType() == DataFieldType.DATE) {
			resolvedFormat = resolve(computedLS.getDateFormat().getValue());
		} else {
			resolvedFormat = resolve(computedLS.getNumberFormat().getValue());
		}
		String resolvedLocale = resolve(computedLS.getLocale().getValue());
		String resolvedTimezone = resolve(computedLS.getTimezone().getValue());
		
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
			if(resolvedFormat.equals(CommonFormats.INTEGER)) {
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
		LanguageSetting originalLS = getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0);
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(originalLS, parentLanguageSetting);
		String resolvedTarget = resolve(target.getValue());
		DataFieldMetadata fieldMetadata = safeGetFieldMetadata(inputMetadata, resolvedTarget);
		if(fieldMetadata == null) {
			return false;
		}
		String resolvedFormat;
		if(fieldMetadata.getDataType() == DataFieldType.DATE) {
			resolvedFormat = resolve(computedLS.getDateFormat().getValue());
		} else {
			resolvedFormat = resolve(computedLS.getNumberFormat().getValue());
		}
		String resolvedLocale = resolve(computedLS.getLocale().getValue());
		String resolvedTimezone = resolve(computedLS.getTimezone().getValue());
		
		if(fieldMetadata != null && fieldMetadata.getDataType() != DataFieldType.STRING) {
			if(fieldMetadata.getDataType() == DataFieldType.DATE) {
				state &= isLocaleReady(resolvedLocale, originalLS.getLocale(), accumulator);
				state &= isDateFormatReady(resolvedFormat, originalLS.getDateFormat(), accumulator);
				state &= isTimezoneReady(resolvedTimezone, originalLS.getTimezone(), accumulator);	
			}
			if(fieldMetadata.getDataType() == DataFieldType.INTEGER || fieldMetadata.getDataType() == DataFieldType.LONG || fieldMetadata.getDataType() == DataFieldType.NUMBER || fieldMetadata.getDataType() == DataFieldType.DECIMAL) {
				state &= isLocaleReady(resolvedLocale, originalLS.getLocale(), accumulator);
				state &= isNumberFormatReady(resolvedFormat, originalLS.getNumberFormat(), accumulator);
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
}

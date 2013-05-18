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

import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.DecimalDataField;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.CloverString;

/**
 * <p>Shared based for validation rules which perform validation on strings (such as {@link PatternMatchValidationRule}).
 * Provides method for preparing field value into string with respect to language setting and possible trimming.</p>
 * 
 * Available settings:
 * <ul>
 * 	<li>Trim input. If the input should be trimmed.</li>
 * </ul>
 * 
 * How to use:
 * <ul>
 * 	<li>Inherit from this class.</li>
 *  <li>Use via {@link #prepareInput()}</li>
 * </ul>
 * 
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

	public List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		final DataRecordMetadata inputMetadata = inMetadata;
		
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		trimInput.setName("Trim input");
		params.add(trimInput);
		trimInput.setEnabledHandler(new EnabledHandler() {
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
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
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && (fieldMetadata.getDataType() == DataFieldType.INTEGER || fieldMetadata.getDataType() == DataFieldType.LONG || fieldMetadata.getDataType() == DataFieldType.NUMBER || fieldMetadata.getDataType() == DataFieldType.DECIMAL)) {
					return true;
				}
				return false;
			}
		});
		
		languageSetting.getDateFormat().setEnabledHandler(new EnabledHandler() {
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && (fieldMetadata.getDataType() == DataFieldType.DATE)) {
					return true;
				}
				return false;
			}
		});
		
		languageSetting.getLocale().setEnabledHandler(new EnabledHandler() {
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() != DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		languageSetting.getTimezone().setEnabledHandler(new EnabledHandler() {
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() == DataFieldType.DATE) {
					return true;
				}
				return false;
			}
		});
		
		return params;
	}
	
	/**
	 * Takes care about getting string from field of given name.
	 * Method take care about converting non-string fields with respect to language setting. 
	 * @param record Whole record
	 * @param name Name of obtained field
	 * @return Not null output string.
	 */
	protected String prepareInput(DataRecord record, String name) {
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0), parentLanguageSetting);
		
		DataFieldMetadata fieldMetadata = record.getMetadata().getField(name);
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
			return "";	// string is wanted!
		}
		Object value = field.getValue();
		if(value instanceof Boolean) {
			if((Boolean) value) {
				return "True";
			} else {
				return "False";
			}
			
		} else if (value instanceof Date) {
			String formatString;
			if(resolvedFormat.isEmpty()) {
				formatString = Defaults.DEFAULT_DATETIME_FORMAT;
			} else {
				formatString = resolvedFormat;
			}
			SimpleDateFormat dateFormat = new SimpleDateFormat(formatString, ValidatorUtils.localeFromString(resolvedLocale));
			dateFormat.setTimeZone(TimeZone.getTimeZone(resolvedTimezone));
			return dateFormat.format(((DateDataField) field).getValue());
		} else if (value instanceof Integer || value instanceof Integer || value instanceof Decimal || value instanceof Double) {
			if(resolvedFormat.isEmpty()) {
				return field.getValue().toString();
			}
			DecimalFormat decimalFormat;
			if(resolvedFormat.equals(CommonFormats.INTEGER)) {
				decimalFormat = (DecimalFormat) DecimalFormat.getIntegerInstance(ValidatorUtils.localeFromString(resolvedLocale));
				decimalFormat.applyPattern("#");
				decimalFormat.setGroupingUsed(false); // Suppress grouping of thousand by default
			} else if(resolvedFormat.equals(CommonFormats.NUMBER)) {
				decimalFormat = (DecimalFormat) DecimalFormat.getInstance(ValidatorUtils.localeFromString(resolvedLocale));
				decimalFormat.setGroupingUsed(false); // Suppress grouping of thousand by default
			} else {
				decimalFormat = (DecimalFormat) DecimalFormat.getInstance(ValidatorUtils.localeFromString(resolvedLocale));
				decimalFormat.applyPattern(resolvedFormat);
			}
			if(value instanceof Decimal) {
				return decimalFormat.format(((DecimalDataField) field).getBigDecimal());
			} else {
				return decimalFormat.format(field.getValue());
			}
		} else if (value instanceof byte[]) {
			try {
				return new String((byte[]) value, "UTF-8");
			} catch (UnsupportedEncodingException ex) {
				// Should not really happen
				logger.error("Conversion byte[] to String failed due to unsupported encoding");
				return "";
			}
		} else if (value instanceof String || value instanceof CloverString) {
			String temp = value.toString();
			if(trimInput.getValue()) {
				temp = temp.trim();
			}
			return temp;
		} else {
			return value.toString();
		}
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		setPropertyRefResolver(graphWrapper);
		boolean state = true;
		LanguageSetting originalLS = getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0);
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(originalLS, parentLanguageSetting);
		String resolvedTarget = resolve(target.getValue());
		DataFieldMetadata fieldMetadata = inputMetadata.getField(resolvedTarget);
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
	 * @return Param node with trim settings
	 */
	public BooleanValidationParamNode getTrimInput() {
		return trimInput;
	}
}

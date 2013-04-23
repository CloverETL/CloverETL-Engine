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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.ChangeHandler;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.component.validator.utils.comparators.DateComparator;
import org.jetel.component.validator.utils.comparators.DecimalComparator;
import org.jetel.component.validator.utils.comparators.DoubleComparator;
import org.jetel.component.validator.utils.comparators.LongComparator;
import org.jetel.component.validator.utils.comparators.StringComparator;
import org.jetel.component.validator.utils.convertors.Converter;
import org.jetel.component.validator.utils.convertors.DateConverter;
import org.jetel.component.validator.utils.convertors.DecimalConverter;
import org.jetel.component.validator.utils.convertors.DoubleConverter;
import org.jetel.component.validator.utils.convertors.LongConverter;
import org.jetel.component.validator.utils.convertors.StringConverter;
import org.jetel.data.DataField;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Class providing easy to use conversion utils for validation rule.
 * Default type to use is choosen from metadata, but it can be overriden by user.
 * Usage:
 *  - Extend from this class
 *  - In method {@link #isValid(org.jetel.data.DataRecord, org.jetel.component.validator.ValidationErrorAccumulator, GraphWrapper)} perform this 
 * 	<code>
 * 		DataFieldType fieldType = computeType(field);
 *		try {
 *			initConversionUtils(fieldType);
 *		} catch (IllegalArgumentException ex) {
 *			raiseError(ea, ERROR_INIT_CONVERSION, "Cannot initialize conversion and comparator tools.", nodePath, resolvedTarget, field.getValue().toString());
 *			return State.INVALID;
 *		}
 *	</code>
 * - In method {@link #isReady(DataRecordMetadata, ReadynessErrorAcumulator, GraphWrapper)} do not forget check for language setting by calling this
 * 	<code>
 * 		state &= super.isReady(inputMetadata, accumulator, graphWrapper);
 * 	</code>
 * - Use via <code>tempComparator</code> and <code>tempConverter</code>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 12.3.2013
 */
@XmlType(propOrder={"useTypeJAXB"})
public abstract class ConversionValidationRule extends LanguageSettingsValidationRule {
	
	protected static final int LANGUAGE_SETTING_ACCESSOR_0 = 0;

	public static enum METADATA_TYPES {
		DEFAULT, STRING, DATE, LONG, NUMBER, DECIMAL;
		@Override
		public String toString() {
			if(this.equals(DEFAULT)) {
				return "type from metadata";
			}
			if(this.equals(STRING)) {
				return "strings";
			}
			if(this.equals(DATE)) {
				return "dates";
			}
			if(this.equals(LONG)) {
				return "longs";
			}
			if(this.equals(NUMBER)) {
				return "numbers";
			}
			return "decimals";
		}
	}
	
	protected EnumValidationParamNode useType = new EnumValidationParamNode(METADATA_TYPES.values(), METADATA_TYPES.DEFAULT);
	@XmlElement(name="useType")
	@SuppressWarnings("unused")
	private String getUseTypeJAXB() { return ((Enum<?>) useType.getValue()).name(); }
	@SuppressWarnings("unused")
	private void setUseTypeJAXB(String input) { this.useType.setFromString(input); }
	
	public ConversionValidationRule() {
		addLanguageSetting(new LanguageSetting());
	}
		
	protected Converter tempConverter;
	protected Comparator<?> tempComparator;
	
	@Override
	protected List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		final DataRecordMetadata inputMetadata = inMetadata;
		
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		useType.setName("Compare as");
		params.add(useType);
		
		LanguageSetting languageSetting = getLanguageSettings(0);
		languageSetting.initialize();
		languageSetting.getDateFormat().setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && (useType.getValue() == METADATA_TYPES.DATE) && fieldMetadata.getDataType() == DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		languageSetting.getNumberFormat().setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && (useType.getValue() == METADATA_TYPES.NUMBER || useType.getValue() == METADATA_TYPES.DECIMAL) && fieldMetadata.getDataType() == DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		languageSetting.getLocale().setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && (useType.getValue() != METADATA_TYPES.STRING && useType.getValue() != METADATA_TYPES.DEFAULT && useType.getValue() != METADATA_TYPES.LONG) && fieldMetadata.getDataType() == DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		languageSetting.getTimezone().setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && (useType.getValue() == METADATA_TYPES.DATE) && fieldMetadata.getDataType() == DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		return params;
	}
	
	public void initConversionUtils(DataFieldType fieldType) {
		if(tempComparator == null) {
			if (fieldType == DataFieldType.STRING) {
				tempComparator = StringComparator.getInstance();
			} else if (fieldType == DataFieldType.INTEGER
					|| fieldType == DataFieldType.LONG
					|| fieldType == DataFieldType.BYTE
					|| fieldType == DataFieldType.CBYTE
					|| fieldType == DataFieldType.BOOLEAN) {
				tempComparator = LongComparator.getInstance();
			} else if (fieldType == DataFieldType.DATE) {
				tempComparator = DateComparator.getInstance();
			} else if (fieldType == DataFieldType.NUMBER) {
				tempComparator = DoubleComparator.getInstance();
			} else if (fieldType == DataFieldType.DECIMAL) {
				tempComparator = DecimalComparator.getInstance();
			} else {
				throw new IllegalArgumentException("Cannot determine comparator for validation.");
			}
		}
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0), parentLanguageSetting);
		
		String resolvedDateFormat = resolve(computedLS.getDateFormat().getValue());
		String resolvedNumberFormat = resolve(computedLS.getNumberFormat().getValue());
		String resolvedLocale = resolve(computedLS.getLocale().getValue());
		String resolvedTimezone = resolve(computedLS.getTimezone().getValue());
		if(tempConverter == null) {
			if (fieldType == DataFieldType.STRING) {
				tempConverter = StringConverter.getInstance();
			} else if (fieldType == DataFieldType.INTEGER
					|| fieldType == DataFieldType.LONG
					|| fieldType == DataFieldType.BYTE
					|| fieldType == DataFieldType.CBYTE
					|| fieldType == DataFieldType.BOOLEAN) {
				tempConverter = LongConverter.getInstance();
			} else if (fieldType == DataFieldType.DATE) {
				tempConverter = DateConverter.newInstance(resolvedDateFormat, ValidatorUtils.localeFromString(resolvedLocale), TimeZone.getTimeZone(resolvedTimezone));
			} else if (fieldType == DataFieldType.NUMBER) {
				tempConverter = DoubleConverter.newInstance(resolvedNumberFormat, ValidatorUtils.localeFromString(resolvedLocale));
			} else if (fieldType == DataFieldType.DECIMAL) {
				tempConverter = DecimalConverter.newInstance(resolvedNumberFormat, ValidatorUtils.localeFromString(resolvedLocale));
			} else {
				throw new IllegalArgumentException("Cannot determine converter for validation.");
			}
		}
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		setPropertyRefResolver(graphWrapper);
		boolean status = true;
		LanguageSetting originalLS = getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0);
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(originalLS, parentLanguageSetting);
		String resolvedTarget = resolve(target.getValue());
		DataFieldMetadata fieldMetadata = inputMetadata.getField(resolvedTarget);
		if(fieldMetadata == null) {
			return false;
		}
		
		String resolvedNumberFormat = resolve(computedLS.getNumberFormat().getValue());
		String resolvedDateFormat = resolve(computedLS.getDateFormat().getValue());
		String resolvedLocale = resolve(computedLS.getLocale().getValue());
		String resolvedTimezone = resolve(computedLS.getTimezone().getValue());
		if(fieldMetadata != null && (useType.getValue() == METADATA_TYPES.DATE) && fieldMetadata.getDataType() == DataFieldType.STRING) {
			status &= isDateFormatReady(resolvedDateFormat, originalLS.getDateFormat(), accumulator);
			status &= isLocaleReady(resolvedLocale, originalLS.getLocale(), accumulator);
			status &= isTimezoneReady(resolvedTimezone, originalLS.getTimezone(), accumulator);
		}
		if(fieldMetadata != null && (useType.getValue() == METADATA_TYPES.DECIMAL || useType.getValue() == METADATA_TYPES.NUMBER || useType.getValue() == METADATA_TYPES.LONG) && fieldMetadata.getDataType() == DataFieldType.STRING) {
			status &= isNumberFormatReady(resolvedNumberFormat, originalLS.getNumberFormat(), accumulator);
			status &= isLocaleReady(resolvedLocale, originalLS.getLocale(), accumulator);
			status &= isTimezoneReady(resolvedTimezone, originalLS.getTimezone(), accumulator);
		}
		return status;
	}
	
	protected DataFieldType computeType(DataField field) {
		DataFieldType fieldType = field.getMetadata().getDataType();
		if(useType.getValue() == METADATA_TYPES.STRING) {
			fieldType = DataFieldType.STRING;
		} else if(useType.getValue() == METADATA_TYPES.DATE) {
			fieldType = DataFieldType.DATE;
		} else if(useType.getValue() == METADATA_TYPES.LONG) {
			fieldType = DataFieldType.LONG;
		} else if(useType.getValue() == METADATA_TYPES.NUMBER) {
			fieldType = DataFieldType.NUMBER;
		} else if(useType.getValue() == METADATA_TYPES.DECIMAL) {
			fieldType = DataFieldType.DECIMAL;
		}
		return fieldType;
	}

	public EnumValidationParamNode getUseType() {
		return useType;
	}
}


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

import java.util.Collection;
import java.util.Comparator;
import java.util.TimeZone;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
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
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * <p>Class providing easy to use conversion utils for validation rules.</p>
 * 
 * <p>Default type to be used is chosen from incoming metadata, but it can be overridden by user selection.</p>
 * 
 * Available settings:
 * <ul>
 *  <li>Type to be used. According to this parameter the converter and comparator are initialized.</li>
 * </ul>
 * 
 * Usage:
 * <ul>
 *  <li>Extend from this class</li>
 *  <li>In method {@link #isValid()} perform this<br /> 
 * 	 <code>
 * 		DataFieldType fieldType = computeType(field);
 *		try {
 *			initConversionUtils(fieldType);
 *		} catch (IllegalArgumentException ex) {
 *			raiseError(...);
 *			return State.INVALID;
 *	 	}
 *	 </code>
 *  </li>
 *  <li>In method {@link #isReady()} do not forget check correctness of language setting by calling this
 * 	<code>
 * 		state &= super.isReady(inputMetadata, accumulator, graphWrapper);
 * 	</code>
 *  <li>Use via <code>tempComparator</code> and <code>tempConverter</code></li>
 *  <li>For proper initialization of param nodes in GUI perform <code>super.initialize(...);</code> in child class.</li>
 * </ul>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 12.3.2013
 * @see LanguageSettingsValidationRule
 */
@XmlType(propOrder={"useTypeJAXB"})
public abstract class ConversionValidationRule<T> extends LanguageSettingsValidationRule {
	
	protected static final int LANGUAGE_SETTING_ACCESSOR_0 = 0;

	/**
	 * Available types (each have associated converter and comparator)
	 */
	public static enum METADATA_TYPES {
		DEFAULT, STRING, DATE, LONG, NUMBER, DECIMAL;
		@Override
		public String toString() {
			if(this.equals(DEFAULT)) {
				return ValidatorMessages.getString("ConversionValidationRule.DefaultConversionType"); //$NON-NLS-1$
			}
			if(this.equals(STRING)) {
				return "strings"; //$NON-NLS-1$
			}
			if(this.equals(DATE)) {
				return "dates"; //$NON-NLS-1$
			}
			if(this.equals(LONG)) {
				return "longs"; //$NON-NLS-1$
			}
			if(this.equals(NUMBER)) {
				return "numbers"; //$NON-NLS-1$
			}
			return "decimals"; //$NON-NLS-1$
		}
	}
	
	protected EnumValidationParamNode<METADATA_TYPES> useType = new EnumValidationParamNode<METADATA_TYPES>(METADATA_TYPES.values(), METADATA_TYPES.DEFAULT);
	@XmlElement(name="useType")
	@SuppressWarnings("unused")
	private String getUseTypeJAXB() { return ((Enum<?>) useType.getValue()).name(); }
	@SuppressWarnings("unused")
	private void setUseTypeJAXB(String input) { this.useType.setFromString(input); }
	
	public ConversionValidationRule() {
		addLanguageSetting(new LanguageSetting());
	}
		
	protected Converter tempConverter;
	protected Comparator<T> tempComparator;
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		final DataRecordMetadata inputMetadata = inMetadata;
		
		useType.setName(ValidatorMessages.getString("ConversionValidationRule.UseTypeParameterName")); //$NON-NLS-1$
		
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
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(useType);
	}
	
	protected void initConversionUtils(DataFieldType fieldType) {
		if(tempComparator == null) {
			if (fieldType == DataFieldType.STRING) {
				tempComparator = (Comparator<T>) StringComparator.getInstance();
			} else if (fieldType == DataFieldType.INTEGER
					|| fieldType == DataFieldType.LONG
					|| fieldType == DataFieldType.BYTE
					|| fieldType == DataFieldType.CBYTE
					|| fieldType == DataFieldType.BOOLEAN) {
				tempComparator = (Comparator<T>) LongComparator.getInstance();
			} else if (fieldType == DataFieldType.DATE) {
				tempComparator = (Comparator<T>) DateComparator.getInstance();
			} else if (fieldType == DataFieldType.NUMBER) {
				tempComparator = (Comparator<T>) DoubleComparator.getInstance();
			} else if (fieldType == DataFieldType.DECIMAL) {
				tempComparator = (Comparator<T>) DecimalComparator.getInstance();
			} else {
				throw new IllegalArgumentException(ValidatorMessages.getString("ConversionValidationRule.IllegalComparatorError")); //$NON-NLS-1$
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
				throw new IllegalArgumentException(ValidatorMessages.getString("ConversionValidationRule.IllegalConverterError")); //$NON-NLS-1$
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
	
	protected DataFieldType computeType(DataFieldType fieldType) {
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


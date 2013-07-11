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
import java.text.MessageFormat;
import java.text.ParsePosition;
import java.util.Collection;
import java.util.Locale;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

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
	
	private int fieldPosition;
	
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	private DecimalFormat numberFormat;
	private String resolvedTarget;

	public NumberValidationRule() {
		addLanguageSetting(new LanguageSetting());
	}
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		trimInput.setName(ValidatorMessages.getString("NumberValidationRule.TrimInputParameterName")); //$NON-NLS-1$
		trimInput.setTooltip(ValidatorMessages.getString("NumberValidationRule.TrimInputTooltip")); //$NON-NLS-1$
		
		LanguageSetting languageSetting = getLanguageSettings(0);
		languageSetting.initialize();
		languageSetting.getDateFormat().setHidden(true);
		languageSetting.getTimezone().setHidden(true);
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
	public void init(DataRecordMetadata metadata, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(metadata, graphWrapper);
		
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0), parentLanguageSetting);
		
		resolvedTarget = (target.getValue());
		String resolvedFormat = (computedLS.getNumberFormat().getValue());
		String resolvedLocale = (computedLS.getLocale().getValue());
		
		fieldPosition = metadata.getFieldPosition(resolvedTarget);
		if (fieldPosition == -1) {
			throw new ComponentNotReadyException(MessageFormat.format(ValidatorMessages.getString("NumberValidationRule.InitErrorTargetFieldMissing"), resolvedTarget)); //$NON-NLS-1$
		}
		DataFieldMetadata fieldMetadata = metadata.getField(fieldPosition);
		if(fieldMetadata.getDataType() != DataFieldType.STRING) {
			throw new ComponentNotReadyException(MessageFormat.format(ValidatorMessages.getString("NumberValidationRule.InitErrorTargetNotAString"), resolvedTarget)); //$NON-NLS-1$
		}
		
		Locale realLocale = ValidatorUtils.localeFromString(resolvedLocale);
		numberFormat = (DecimalFormat) DecimalFormat.getInstance(realLocale);
		// Special handling with two named formatting masks
		if(computedLS.getNumberFormat().getValue().equals(CommonFormats.INTEGER)) {
			numberFormat.applyPattern("#"); //$NON-NLS-1$
			numberFormat.setParseIntegerOnly(true);
		} else if(computedLS.getNumberFormat().getValue().equals(CommonFormats.NUMBER)) {
				numberFormat.applyPattern("#"); //$NON-NLS-1$
		} else if(!computedLS.getNumberFormat().getValue().isEmpty()) {
			numberFormat.applyPattern(resolvedFormat);
		}
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return State.NOT_VALIDATED;
		}
		
		DataField field = record.getField(fieldPosition);
		if(field.isNull()) {
			return State.VALID;
		}
		String tempString = field.toString();
		if(trimInput.getValue()) {
			tempString = tempString.trim();
		}

		try {
			ParsePosition pos = new ParsePosition(0);
			Number parsedNumber = numberFormat.parse(tempString, pos);
			if(parsedNumber == null || pos.getIndex() != tempString.length()) {
				if (ea != null)
					raiseError(ea, ERROR_PARSING, ValidatorMessages.getString("NumberValidationRule.InvalidRecordMessageCannotParseAsNumber"), resolvedTarget, tempString); //$NON-NLS-1$
				return State.INVALID;
			}
			return State.VALID;
		} catch (Exception ex) {
			if (ea != null)
				raiseError(ea, ERROR_PARSING, ValidatorMessages.getString("NumberValidationRule.InvalidRecordMessageCannotParseAsNumber"), resolvedTarget, tempString); //$NON-NLS-1$
			return State.INVALID;
		}
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		LanguageSetting originalLS = getLanguageSettings(LANGUAGE_SETTING_ACCESSOR_0);
		LanguageSetting computedLS = LanguageSetting.hierarchicMerge(originalLS, parentLanguageSetting);
		
		String resolvedTarget = (target.getValue());
		String resolvedFormat = (computedLS.getNumberFormat().getValue());
		String resolvedLocale = (computedLS.getLocale().getValue());
		
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, ValidatorMessages.getString("NumberValidationRule.ConfigurationErrorTargetEmpty")); //$NON-NLS-1$
			state = false;
		} else {
			if(inputMetadata.getField(resolvedTarget) != null && inputMetadata.getField(resolvedTarget).getDataType() != DataFieldType.STRING) {
				accumulator.addError(target, this, ValidatorMessages.getString("NumberValidationRule.ConfigurationErrorTargetNotAString")); //$NON-NLS-1$
				state = false;	
			}
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, ValidatorMessages.getString("NumberValidationRule.ConfigurationErrorTargetMissing")); //$NON-NLS-1$
			state = false;
		}
		
		state &= isLocaleReady(resolvedLocale, originalLS.getLocale(), accumulator);
		state &= isNumberFormatReady(resolvedFormat, originalLS.getNumberFormat(), accumulator);

		return state;
	}

	@Override
	public String getCommonName() {
		return ValidatorMessages.getString("NumberValidationRule.CommonName"); //$NON-NLS-1$
	}

	@Override
	public String getCommonDescription() {
		return ValidatorMessages.getString("NumberValidationRule.CommonDescription"); //$NON-NLS-1$
	}
}

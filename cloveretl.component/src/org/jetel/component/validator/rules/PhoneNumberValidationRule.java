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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.rules.PhoneNumberPattern.PhoneNumberPatternFormatException;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.NumberParseException.ErrorType;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;

/**
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30.5.2013
 */
@XmlRootElement(name="phoneNumber")
@XmlType(propOrder={"region", "pattern"})
public class PhoneNumberValidationRule extends StringValidationRule {

	private static final int ERROR_CODE_CANNOT_PARSE = 1401;
	private static final int ERROR_CODE_INVALID_PHONE_NUMBER = 1402;
	private static final int ERROR_CODE_PATTERN_MISMATCH = 1403;
	private static final int ERROR_PHONE_NUMBER_EMPTY = 1404;
	
	private static final Map<String, String> countryCodeToCountryMap;
	static {
		countryCodeToCountryMap = new HashMap<String, String>();
		
		countryCodeToCountryMap.put("AC", "Ascension Island");
		countryCodeToCountryMap.put("SS", "South Sudan");
		String[] isoCountries = Locale.getISOCountries();
		for (String isoCountry : isoCountries) {
			Locale locale = new Locale("", isoCountry);
			countryCodeToCountryMap.put(isoCountry, locale.getDisplayCountry(Locale.ENGLISH));
		}
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}
	
	@XmlElement(name="region",required=false)
	private StringEnumValidationParamNode region = new StringEnumValidationParamNode();
	
	@XmlElement(name="pattern",required=false)
	private StringEnumValidationParamNode pattern = new StringEnumValidationParamNode();

	private PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
	private PhoneNumberPattern requiredPhoneNumberPattern;
	
	private String resolvedRegion;
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		phoneUtil = PhoneNumberUtil.getInstance();
		
		region.setName("Region");
		region.setPlaceholder("None (international phone number expected)");
		pattern.setName("Phone number pattern");
		pattern.setPlaceholder("None (no strict format required)");
		
		String[] regions = phoneUtil.getSupportedRegions().toArray(new String[0]);
		
		Arrays.sort(regions);
		String[] regionsWithEmptyOption = new String[regions.length + 1];
		regionsWithEmptyOption[0] = "";
		
		for (int i = 0; i < regions.length; i++) {
			String regionCode = regions[i];
			String formattedRegion = regionCode;
			String countryName = countryCodeToCountryMap.get(regionCode);
			if (countryName != null) {
				formattedRegion = formattedRegion + " - " + countryName;
			}
			formattedRegion = formattedRegion + " (+" + phoneUtil.getCountryCodeForRegion(regionCode) + ")";
			
			regionsWithEmptyOption[i + 1] = formattedRegion;
		}
		
		
		region.setOptions(regionsWithEmptyOption);
		
		pattern.setOptions(CommonFormats.phoneNumbers);
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(region);
		parametersContainer.add(pattern);
	}
	
	@Override
	public void init(DataRecordMetadata metadata, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(metadata, graphWrapper);
		
		String patternValue = pattern.getValue();
		if (patternValue != null && !patternValue.isEmpty()) {
			try {
				requiredPhoneNumberPattern = PhoneNumberPattern.create(patternValue);
			} catch (PhoneNumberPatternFormatException e) {
				throw new JetelRuntimeException(e);
			}
		}
		else {
			requiredPhoneNumberPattern = null;
		}
		
		resolvedRegion = getRegionCode();
	}

	private String getRegionCode() {
		String resolvedRegion = region.getValue();
		if (resolvedRegion != null && resolvedRegion.length() > 2) {
			resolvedRegion = resolvedRegion.substring(0, 2).toUpperCase();
		}
		return resolvedRegion;
	}
	
	
	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		
		String inputString = prepareInput(record);
		if (inputString == null || inputString.isEmpty()) {
			if (ea != null) {
				raiseError(ea, ERROR_PHONE_NUMBER_EMPTY, "Empty string where phone number was expected", resolvedTarget, inputString);
			}
			return State.INVALID;
		}
		
		PhoneNumber phoneNumber;
		try {
			phoneNumber = phoneUtil.parse(inputString, resolvedRegion);
		} catch (NumberParseException e) {
			if (ea != null) {
				if (e.getErrorType() == ErrorType.INVALID_COUNTRY_CODE && (resolvedRegion == null || resolvedRegion.isEmpty())) {
					raiseError(ea, ERROR_CODE_CANNOT_PARSE, "Phone number starts with invalid country code and no region is specified", resolvedTarget, inputString);
				}
				else {
					raiseError(ea, ERROR_CODE_CANNOT_PARSE, e.getMessage(), resolvedTarget, inputString);
				}
			}
			return State.INVALID;
		}
		if (!phoneUtil.isValidNumber(phoneNumber)) {
			if (ea != null)
				raiseError(ea, ERROR_CODE_INVALID_PHONE_NUMBER, "Invalid phone number", resolvedTarget, inputString);
			return State.INVALID;
		}
		if (requiredPhoneNumberPattern != null) {
			if (!requiredPhoneNumberPattern.matches(inputString)) {
				if (ea != null)
					raiseError(ea, ERROR_CODE_PATTERN_MISMATCH, "Phone number doesn't match the required pattern", resolvedTarget, inputString);
				return State.INVALID;
			}
		}
		
		return State.VALID;
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator,
			GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		String resolvedTarget = target.getValue();
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		String patternValue = pattern.getValue();
		if (patternValue != null && !patternValue.isEmpty()) {
			try {
				PhoneNumberPattern.create(patternValue);
			} catch (PhoneNumberPatternFormatException e) {
				accumulator.addError(target, this, e.getMessage());
				state = false;
			}
		}
		resolvedRegion = getRegionCode();
		if (resolvedRegion != null && !resolvedRegion.isEmpty()) {
			if (!phoneUtil.getSupportedRegions().contains(resolvedRegion)) {
				accumulator.addError(target, this, "Unknown region " + resolvedRegion);
				state = false;
			}
		}
		state &= super.isReady(inputMetadata, accumulator, graphWrapper);
		return state;
	}

	@Override
	public String getCommonName() {
		return "Phone number";
	}

	@Override
	public String getCommonDescription() {
		return "Checks whether given string is a valid phone number";
	}
	
	public StringEnumValidationParamNode getRegion() {
		return region;
	}
	
	public StringEnumValidationParamNode getPattern() {
		return pattern;
	}

}

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
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.rules.PhoneNumberPattern.PhoneNumberPatternFormatException;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

import com.google.i18n.phonenumbers.NumberParseException;
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

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}
	
	@XmlElement(name="region",required=false)
	private StringEnumValidationParamNode region = new StringEnumValidationParamNode();
	
	@XmlElement(name="pattern",required=false)
	private StringValidationParamNode pattern = new StringValidationParamNode();

	private PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
	private PhoneNumberPattern requiredPhoneNumberPattern;
	
	private String resolvedRegion;
	private String resolvedTarget;
	private boolean initialized = false;
	
	private void init() {
		if (initialized) {
			return;
		}
		initialized = true;
		resolvedTarget = resolve(target.getValue());
		String patternValue = resolve(pattern.getValue());
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
		resolvedRegion = resolve(region.getValue());
	}
	
	@Override
	protected List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		setPropertyRefResolver(graphWrapper);
		phoneUtil = PhoneNumberUtil.getInstance();
		
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		
		params.add(region);
		params.add(pattern);
		region.setName("Region");
		pattern.setName("Required pattern");
		String[] regions = phoneUtil.getSupportedRegions().toArray(new String[0]);
		Arrays.sort(regions);
		region.setOptions(regions);
		
		params.addAll(super.initialize(inMetadata, graphWrapper));
		return params;
	}
	
	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		if (logger.isTraceEnabled()) {
			logParams(StringUtils.mapToString(getProcessedParams(record.getMetadata(), graphWrapper), "=", "\n"));
		}
		init();
		
		String inputString = prepareInput(record, resolvedTarget);
		
		PhoneNumber phoneNumber;
		try {
			phoneNumber = phoneUtil.parse(inputString, resolvedRegion);
		} catch (NumberParseException e) {
			raiseError(ea, ERROR_CODE_CANNOT_PARSE, e.getMessage(), graphWrapper.getNodePath(this), resolvedTarget, inputString);
			return State.INVALID;
		}
		if (!phoneUtil.isValidNumber(phoneNumber)) {
			raiseError(ea, ERROR_CODE_INVALID_PHONE_NUMBER, "Invalid phone number", graphWrapper.getNodePath(this), resolvedTarget, inputString);
			return State.INVALID;
		}
		if (requiredPhoneNumberPattern != null) {
			if (!requiredPhoneNumberPattern.matches(inputString)) {
				raiseError(ea, ERROR_CODE_PATTERN_MISMATCH, "Phone number doesn't match the required pattern", graphWrapper.getNodePath(this), resolvedTarget, inputString);
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
		setPropertyRefResolver(graphWrapper);
		boolean state = true;
		String resolvedTarget = resolve(target.getValue());
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		String patternValue = resolve(pattern.getValue());
		if (patternValue != null && !patternValue.isEmpty()) {
			try {
				PhoneNumberPattern.create(patternValue);
			} catch (PhoneNumberPatternFormatException e) {
				accumulator.addError(target, this, e.getMessage());
				state = false;
			}
		}
		resolvedRegion = resolve(region.getValue());
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
	
	public StringValidationParamNode getPattern() {
		return pattern;
	}

}

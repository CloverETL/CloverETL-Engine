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
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.AbstractValidationRule.TARGET_TYPE;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="patternMatch")
@XmlType(propOrder={"ignoreCase", "pattern"})
public class PatternMatchValidationRule extends StringValidationRule {
	
	@XmlElement(name="ignoreCase",required=true)
	private BooleanValidationParamNode ignoreCase = new BooleanValidationParamNode(false);
	@XmlElement(name="pattern",required=true)
	private StringValidationParamNode pattern = new StringValidationParamNode();
	
	public List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		pattern.setName("Pattern to match");
		pattern.setPlaceholder("Regular expression, for syntax see documentation");
		params.add(pattern);
		ignoreCase.setName("Ignore case");
		params.add(ignoreCase);
		params.addAll(super.initialize(inMetadata, graphWrapper));
		return params;
	}
	

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		logger.trace("Validation rule: " + this.getName() + "\n"
				+ "Target field: " + target.getValue() + "\n"
				+ "Ignore case: " + ignoreCase.getValue() + "\n"
				+ "Pattern: " + pattern.getValue() + "\n"
				+ "Formatting mask: " + format.getValue() + "\n"
				+ "Locale: " + locale.getValue() + "\n"
				+ "Timezone: " + timezone.getValue() + "\n"
				+ "Trim input: " + trimInput.getValue()
				);
		
		String tempString = null;
		try {
			tempString = prepareInput(record, target.getValue());
		} catch (IllegalArgumentException ex) {
			logger.trace("Validation rule: " + getName() + " on '" + tempString + "' is " + State.INVALID + " (unknown field)");
			return State.INVALID;
		}
		System.err.println("In string: " + tempString);
		Pattern pm;
		try {
			if(ignoreCase.getValue()) {
				pm = Pattern.compile(pattern.getValue(), Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
			} else {
				pm = Pattern.compile(pattern.getValue(), Pattern.UNICODE_CASE);
			}
		} catch (PatternSyntaxException e) {
			logger.trace("Validation rule: " + getName() + " on '" + tempString + "' is " + State.INVALID + " (pattern is invalid)");
			return State.INVALID;
		}
		if(pm.matcher(tempString).matches()) {
			logger.trace("Validation rule: " + getName() + " on '" + tempString + "' is " + State.VALID);
			return State.VALID;
		} else {
			// TODO: Add error reporting
			logger.trace("Validation rule: " + getName() + " on '" + tempString + "' is " + State.INVALID);
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
		if(pattern.getValue().isEmpty()) {
			accumulator.addError(pattern, this, "Match pattern is empty.");
			state = false;
		}
		if(!ValidatorUtils.isValidField(target.getValue(), inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		state &= super.isReady(inputMetadata, accumulator);
		return state;
	}

	/**
	 * @return the target
	 */
	public StringValidationParamNode getTarget() {
		return target;
	}


	/**
	 * @return the ignoreCase
	 */
	public BooleanValidationParamNode getIgnoreCase() {
		return ignoreCase;
	}


	/**
	 * @return the pattern
	 */
	public StringValidationParamNode getPattern() {
		return pattern;
	}
	
	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}


	@Override
	public String getCommonName() {
		return "Pattern Match";
	}


	@Override
	public String getCommonDescription() {
		return "Checks whether chosen field matches regular expression provided by user.";
	}

}

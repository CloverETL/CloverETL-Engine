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

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.data.DataRecord;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="patternMatch")
public class PatternMatchValidationRule extends StringValidationRule {
	
	public final static int IGNORE_CASE = 100;
	public final static int PATTERN = 101;
	
	@XmlElement(name="target",required=true)
	private StringValidationParamNode target = new StringValidationParamNode(TARGET, "Target field");
	@XmlElement(name="ignoreCase",required=true)
	private BooleanValidationParamNode ignoreCase = new BooleanValidationParamNode(IGNORE_CASE, "Ingore case", false);
	@XmlElement(name="pattern",required=true)
	private StringValidationParamNode pattern = new StringValidationParamNode(PATTERN, "Patern to match");
	
	public PatternMatchValidationRule() {
		super();
		addParamNode(target);
		addParamNode(ignoreCase);
		addParamNode(pattern);
	}
	

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		logger.trace("Validation rule: " + this.getName() + "\n"
				+ "Target field: " + target.getValue() + "\n"
				+ "Ignore case: " + ignoreCase.getValue() + "\n"
				+ "Pattern: " + pattern.getValue() + "\n"
				+ "Trim input: " + trimInput.getValue());
		
		String tempString = prepareInput(record.getField(target.getValue()));
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
	public boolean isReady() {
		return !pattern.getValue().isEmpty() && !target.getValue().isEmpty();
	}

}

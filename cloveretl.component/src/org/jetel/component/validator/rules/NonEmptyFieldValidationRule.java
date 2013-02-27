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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.ValidationError;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.AbstractValidationRule.TARGET_TYPE;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.data.DataRecord;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
@XmlRootElement(name="nonEmptyField")
@XmlType(propOrder={"checkForEmptiness"})
public class NonEmptyFieldValidationRule extends StringValidationRule {
	
	@XmlElement(name="checkForEmptiness",required=true)
	private BooleanValidationParamNode checkForEmptiness = new BooleanValidationParamNode(false);
	
	public List<ValidationParamNode> initialize() {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		checkForEmptiness.setName("Only empty field is valid");
		params.add(checkForEmptiness);
		params.addAll(super.initialize());
		return params;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		String tempString = prepareInput(record.getField(target.getValue()));
		logger.trace("Validation rule: " + this.getName() + "\n"
				+ "Target field: " + target.getValue() + "\n"
				+ "Check for emptiness: " + checkForEmptiness.getValue() + "\n"
				+ "Trim input: " + trimInput.getValue());
		if(checkForEmptiness.getValue() && tempString.isEmpty()) {
			logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.VALID);
			return State.VALID;
		}
		if(!checkForEmptiness.getValue() && !tempString.isEmpty()) {
			logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.VALID);
			return State.VALID;
		}
		if(ea != null) {
			// TODO: Error reporting
			//ea.addError(new Error("NonEmptyRule", "NonEmptyRule failed", getName(), ArrayList(), params, values))
		}
		logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.INVALID);
		return State.INVALID;
	}
	private ValidationError prepareError(String message) {
		List<String> fields = new ArrayList<String>();
		fields.add(target.getValue());
		Map<String, String> params = new HashMap<String, String>();
		if(checkForEmptiness.getValue()) {
			params.put("emptiness", "true");
		} else {
			params.put("emptiness", "false");
		}
		params.put("target", target.getValue());		
		return new ValidationError("100", message, this.getName(), fields, params, new HashMap<String, String>());
	}

	@Override
	public boolean isReady() {
		String targetField = target.getValue();
		return !targetField.isEmpty();
	}

	/**
	 * @return the target
	 */
	public StringValidationParamNode getTarget() {
		return target;
	}

	/**
	 * @return the checkForEmptiness
	 */
	public BooleanValidationParamNode getCheckForEmptiness() {
		return checkForEmptiness;
	}
	
	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}

	@Override
	public String getCommonName() {
		return "Empty/Nonempty field";
	}

	@Override
	public String getCommonDescription() {
		return "Checks whether chosen field is empty or nonempty depending on user choice.";
	}
}

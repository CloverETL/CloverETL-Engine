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

import org.jetel.component.validator.ValidationError;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.data.DataRecord;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
@XmlRootElement(name="nonEmptyField")
public class NonEmptyFieldValidationRule extends StringValidationRule {
	public final static int GOAL = 100;
	
	@XmlElement(name="target",required=true)
	private StringValidationParamNode target = new StringValidationParamNode(TARGET, "Target field");
	@XmlElement(name="checkForEmptiness",required=true)
	private BooleanValidationParamNode checkForEmptiness = new BooleanValidationParamNode(GOAL, "Only empty field is valid", false);
	
	public NonEmptyFieldValidationRule() {
		super();
		addParamNode(target);
		addParamNode(checkForEmptiness);
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
}

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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.data.DataRecord;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="nonEmptySubset")
public class NonEmptySubsetValidationRule extends StringValidationRule {
	
	public final static String GOAL = "goal";
	public final static String COUNT = "count";
	
	@XmlElement(name="target",required=true)
	private StringValidationParamNode target = new StringValidationParamNode(TARGET, "Target field");
	@XmlElement(name="checkForEmptiness")
	private BooleanValidationParamNode checkForEmptiness = new BooleanValidationParamNode(GOAL, "Only empty field is valid", false);
	@XmlElement(name="count")
	private IntegerValidationParamNode count = new IntegerValidationParamNode(COUNT, "Number of fields", 1);
	
	public NonEmptySubsetValidationRule() {
		super();
		addParamNode(target);
		addParamNode(checkForEmptiness);
		addParamNode(count);
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		String tempString;
		logger.trace("Validation rule: " + this.getName() + "\n"
				+ "Target fields: " + target.getValue() + "\n"
				+ "Check for emptiness: " + checkForEmptiness.getValue() + "\n"
				+ "Desired count: " + count.getValue() + "\n"
				+ "Trim input: " + trimInput.getValue());
		
		String[] targetField = parseTargets();
		int ok = 0;
		for(int i = 0; i < targetField.length; i++) {
			tempString = prepareInput(record.getField(targetField[i]));
			if(checkForEmptiness.getValue() && tempString.isEmpty() ||
					!checkForEmptiness.getValue() && !tempString.isEmpty()) {
				ok++;
			}
			if(ok >= count.getValue()) {
				logger.trace("Validation rule: " + getName() + " is " + State.VALID);
				return State.VALID;
			}
		}
		if(ea != null) {
			// TODO: Error reporting
			//ea.addError(new Error("NonEmptyRule", "NonEmptyRule failed", getName(), ArrayList(), params, values))
		}
		logger.trace("Validation rule: " + getName() + " is " + State.INVALID);
		return State.INVALID;
	}

	@Override
	public boolean isReady() {
		return count.getValue() != null && 
				count.getValue() > 0 &&
				parseTargets().length > 0;
	}
	
	private String[] parseTargets() {
		return target.getValue().split(",");
	}

}

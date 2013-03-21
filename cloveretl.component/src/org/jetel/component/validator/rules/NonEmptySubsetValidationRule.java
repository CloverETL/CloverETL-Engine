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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.AbstractValidationRule.TARGET_TYPE;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.rules.StringLengthValidationRule.TYPES;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="nonEmptySubset")
@XmlType(propOrder={"goalJAXB", "count"})
public class NonEmptySubsetValidationRule extends StringValidationRule {
	
	public static enum GOALS {
		EMPTY, NONEMPTY;
		@Override
		public String toString() {
			if(this.equals(EMPTY)) {
				return "Empty fields";
			}
			return "Nonempty fields";
		}
	}
	
	private EnumValidationParamNode goal = new EnumValidationParamNode(GOALS.values(), GOALS.NONEMPTY);
	@XmlElement(name="goal", required=true)
	private String getGoalJAXB() { return ((Enum<?>) goal.getValue()).name(); }
	private void setGoalJAXB(String input) { goal.setFromString(input); }
	
	@XmlElement(name="count",required=true)
	private IntegerValidationParamNode count = new IntegerValidationParamNode(1);
	
	public List<ValidationParamNode> initialize() {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		goal.setName("Count");
		params.add(goal);
		count.setName("Minimal count");
		params.add(count);
		params.addAll(super.initialize());
		return params;
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
				+ "Goal: " + goal.getValue() + "\n"
				+ "Desired count: " + count.getValue() + "\n"
				+ "Trim input: " + trimInput.getValue());
		
		String[] targetField = ValidatorUtils.parseTargets(target.getValue());
		int ok = 0;
		for(int i = 0; i < targetField.length; i++) {
			tempString = prepareInput(record.getField(targetField[i]));
			if(goal.getValue() == GOALS.EMPTY && tempString.isEmpty() ||
					goal.getValue() == GOALS.NONEMPTY && !tempString.isEmpty()) {
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
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		if(target.getValue().isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(count.getValue() == null || count.getValue().compareTo(Integer.valueOf(0)) <= 0) {
			accumulator.addError(count, this, "Count of fields must be greater than zero.");
			state = false;
		}
		if(!ValidatorUtils.areValidFields(target.getValue(), inputMetadata)) { 
			accumulator.addError(target, this, "Some of target fields are not present in input metadata.");
			state = false;
		}
		return state;
	}

	/**
	 * @return the target
	 */
	public StringValidationParamNode getTarget() {
		return target;
	}

	/**
	 * @return the goal
	 */
	public EnumValidationParamNode getGoal() {
		return goal;
	}

	/**
	 * @return the count
	 */
	public IntegerValidationParamNode getCount() {
		return count;
	}
	
	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.UNORDERED_FIELDS;
	}

	@Override
	public String getCommonName() {
		return "Empty/Nonempty subset";
	}

	@Override
	public String getCommonDescription() {
		return "Checks whether at least n chosen fields are empty or nonempty depending on user choice.";
	}

}

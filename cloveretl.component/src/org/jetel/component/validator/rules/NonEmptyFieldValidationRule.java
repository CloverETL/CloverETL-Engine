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

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
@XmlRootElement(name="nonEmptyField")
@XmlType(propOrder={"goalJAXB", "trimInput"})
public class NonEmptyFieldValidationRule extends AbstractValidationRule {
	
	public static int ERROR_FIELD_EMPTY = 101;
	public static int ERROR_FIELD_NONEMPTY = 102;
	
	public static enum GOALS {
		EMPTY, NONEMPTY;
		@Override
		public String toString() {
			if(this.equals(EMPTY)) {
				return "Empty field";
			}
			return "Nonempty field";
		}
	}
	
	private EnumValidationParamNode goal = new EnumValidationParamNode(GOALS.values(), GOALS.NONEMPTY);
	@XmlElement(name="goal", required=true)
	private String getGoalJAXB() { return ((Enum<?>) goal.getValue()).name(); }
	private void setGoalJAXB(String input) { goal.setFromString(input); }
	
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	
	public List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		final DataRecordMetadata inputMetadata = inMetadata;
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		goal.setName("Valid");
		params.add(goal);
		trimInput.setName("Trim input");
		trimInput.setTooltip("Trim input before validation.");
		params.add(trimInput);
		trimInput.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				DataFieldMetadata fieldMetadata = inputMetadata.getField(target.getValue());
				if(fieldMetadata != null && fieldMetadata.getDataType() == DataFieldType.STRING) {
					return true;
				}
				return false;
			}
		});
		return params;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		logParams(StringUtils.mapToString(getProcessedParams(record.getMetadata(), graphWrapper), "=", "\n"));
		
		DataField field = record.getField(target.getValue());
		String inputString = field.toString();
		
		if(field.getMetadata().getDataType() == DataFieldType.STRING) {
			if(trimInput.getValue()) {
				inputString = inputString.trim();
			}
			if(goal.getValue() == GOALS.EMPTY && inputString.isEmpty()) {
				logSuccess("Field '" + target.getValue() + "' is empty.");
				return State.VALID;	
			}
			if(goal.getValue() == GOALS.NONEMPTY && !inputString.isEmpty()) {
				logSuccess("Field '" + target.getValue() + "' with value '" + inputString + "' is nonempty.");
				return State.VALID;	
			} 
		} else if(goal.getValue() == GOALS.EMPTY && field.isNull()) {
			logSuccess("Field '" + target.getValue() + "' is empty.");
			return State.VALID;
		} else if(goal.getValue() == GOALS.NONEMPTY && !field.isNull()) {
			logSuccess("Field '" + target.getValue() + "' with value '" + field.getValue() + "' is nonempty.");
			return State.VALID;
		}
		// Error reporting
		if(goal.getValue() == GOALS.NONEMPTY) {
			raiseError(ea, ERROR_FIELD_EMPTY, "The target field is empty, expected to be nonempty.", target.getValue(), inputString);
		} else {
			raiseError(ea, ERROR_FIELD_NONEMPTY, "The target field is nonempty, expected to be empty.", target.getValue(), inputString);
		}
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
		if(!ValidatorUtils.isValidField(target.getValue(), inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
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
	 * @return the trimInput
	 */
	public BooleanValidationParamNode getTrimInput() {
		return trimInput;
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

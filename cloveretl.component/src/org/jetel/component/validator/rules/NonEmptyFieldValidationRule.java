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

import java.util.Collection;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
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
 * <p>Rule for checking whether given field is null or not null. However when string is on input the rule
 * checks whether this string or this trimmed string is empty/nonempty</p>
 * 
 * Available settings:
 * <ul>
 * 	<li>Goal: Empty/NonEmpty. With selected 'Empty' this rule is valid when incomming value is null.</li>
 *  <li>Trim input: True/False. Available only when incomming field is string.</li>
 * </ul>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 * @see NonEmptySubsetValidationRule
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
	@SuppressWarnings("unused")
	private String getGoalJAXB() { return ((Enum<?>) goal.getValue()).name(); }
	@SuppressWarnings("unused")
	private void setGoalJAXB(String input) { goal.setFromString(input); }
	
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		final DataRecordMetadata inputMetadata = inMetadata;
		goal.setName("Valid");
		trimInput.setName("Trim input");
		trimInput.setTooltip("Trim input before validation.");
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
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(goal);
		parametersContainer.add(trimInput);
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		setPropertyRefResolver(graphWrapper);
		if (logger.isTraceEnabled()) {
			logParams(StringUtils.mapToString(getProcessedParams(record.getMetadata(), graphWrapper), "=", "\n"));
		}
		
		String resolvedTarget = resolve(target.getValue());
		
		DataField field = record.getField(resolvedTarget);
		String inputString = field.toString();
		
		// Special treatment for strings as they can be filled with whitespaces
		if(field.getMetadata().getDataType() == DataFieldType.STRING) {
			if(trimInput.getValue()) {
				inputString = inputString.trim();
			}
			if(goal.getValue() == GOALS.EMPTY && inputString.isEmpty()) {
				logSuccess("Field '" + resolvedTarget + "' is empty.");
				return State.VALID;	
			}
			if(goal.getValue() == GOALS.NONEMPTY && !inputString.isEmpty()) {
				logSuccess("Field '" + resolvedTarget + "' with value '" + inputString + "' is nonempty.");
				return State.VALID;	
			} 
		} else if(goal.getValue() == GOALS.EMPTY && field.isNull()) {
			logSuccess("Field '" + resolvedTarget + "' is empty.");
			return State.VALID;
		} else if(goal.getValue() == GOALS.NONEMPTY && !field.isNull()) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + field.getValue() + "' is nonempty.");
			return State.VALID;
		}
		// Error reporting
		if(goal.getValue() == GOALS.NONEMPTY) {
			raiseError(ea, ERROR_FIELD_EMPTY, "The target field is empty, expected to be nonempty.", graphWrapper.getNodePath(this), resolvedTarget, inputString);
		} else {
			raiseError(ea, ERROR_FIELD_NONEMPTY, "The target field is nonempty, expected to be empty.", graphWrapper.getNodePath(this), resolvedTarget, inputString);
		}
		return State.INVALID;
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
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
		return state;
	}

	/**
	 * @return Param node with currently selected goal
	 */
	public EnumValidationParamNode getGoal() {
		return goal;
	}
	
	/**
	 * @return Param node with current settings of trimming
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

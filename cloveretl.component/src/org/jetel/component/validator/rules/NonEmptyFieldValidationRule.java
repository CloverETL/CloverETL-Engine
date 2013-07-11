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
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

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
				return ValidatorMessages.getString("NonEmptyFieldValidationRule.GoalEmptyFieldLabel"); //$NON-NLS-1$
			}
			return ValidatorMessages.getString("NonEmptyFieldValidationRule.GoalNonEmptyFieldLabel"); //$NON-NLS-1$
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
	private String resolvedTarget;
	private int fieldPosition;
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		final DataRecordMetadata inputMetadata = inMetadata;
		goal.setName(ValidatorMessages.getString("NonEmptyFieldValidationRule.GoalParameterName")); //$NON-NLS-1$
		trimInput.setName(ValidatorMessages.getString("NonEmptyFieldValidationRule.TrimInputParameterName")); //$NON-NLS-1$
		trimInput.setTooltip(ValidatorMessages.getString("NonEmptyFieldValidationRule.TrimInputParameterTooltip")); //$NON-NLS-1$
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
	public void init(DataRecordMetadata metadata, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(metadata, graphWrapper);
		
		resolvedTarget = resolve(target.getValue());
		fieldPosition = metadata.getFieldPosition(resolvedTarget);
		setPropertyRefResolver(graphWrapper);
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return State.NOT_VALIDATED;
		}
		
		DataField field = record.getField(fieldPosition);
		
		// Special treatment for strings as they can be filled with whitespaces
		if(field.getMetadata().getDataType() == DataFieldType.STRING) {
			String inputString = field.toString();
			if(trimInput.getValue()) {
				inputString = inputString.trim();
			}
			if(goal.getValue() == GOALS.EMPTY && inputString.isEmpty()) {
				return State.VALID;	
			}
			if(goal.getValue() == GOALS.NONEMPTY && !inputString.isEmpty()) {
				return State.VALID;	
			} 
		} else if(goal.getValue() == GOALS.EMPTY && field.isNull()) {
			return State.VALID;
		} else if(goal.getValue() == GOALS.NONEMPTY && !field.isNull()) {
			return State.VALID;
		}
		
		if (ea != null) {
			String inputString = field.toString();
			// Error reporting
			if(goal.getValue() == GOALS.NONEMPTY) {
				raiseError(ea, ERROR_FIELD_EMPTY, ValidatorMessages.getString("NonEmptyFieldValidationRule.InvalidRecordMessageNonEmptyExpected"), resolvedTarget, inputString); //$NON-NLS-1$
			} else {
				raiseError(ea, ERROR_FIELD_NONEMPTY, ValidatorMessages.getString("NonEmptyFieldValidationRule.InvalidRecordMessageEmptyExpected"), resolvedTarget, inputString); //$NON-NLS-1$
			}
		}
		return State.INVALID;
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		String resolvedTarget = resolve(target.getValue());
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, ValidatorMessages.getString("NonEmptyFieldValidationRule.ConfigurationErrorTargetEmpty")); //$NON-NLS-1$
			state = false;
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, ValidatorMessages.getString("NonEmptyFieldValidationRule.ConfigurationErrorFieldMissing")); //$NON-NLS-1$
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
		return ValidatorMessages.getString("NonEmptyFieldValidationRule.CommonName"); //$NON-NLS-1$
	}

	@Override
	public String getCommonDescription() {
		return ValidatorMessages.getString("NonEmptyFieldValidationRule.CommonDescription"); //$NON-NLS-1$
	}
}

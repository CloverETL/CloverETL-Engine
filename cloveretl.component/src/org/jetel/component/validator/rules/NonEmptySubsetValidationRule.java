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
import java.util.HashMap;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.IntegerValidationParamNode;
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
 * <p>Rule for checking whether given fields at least given count is null or not null. However when
 * string is on input the rule checks whether this string or this trimmed string is empty/nonempty</p>
 * 
 * Available settings:
 * <ul>
 * 	<li>Goal: Empty/NonEmpty. With selected 'Empty' this rule is valid when incomming value is null.</li>
 *  <li>Count: Number. How many of given fields must be empty/non empty for this rule to be valid</li>
 *  <li>Trim input: True/False. Available only when incomming field is string.</li>
 * </ul>
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 * @see NonEmptyFieldValidationRule
 */
@XmlRootElement(name="nonEmptySubset")
@XmlType(propOrder={"goalJAXB", "count", "trimInput"})
public class NonEmptySubsetValidationRule extends AbstractValidationRule {
	
	public static int ERROR_NOT_ENOUGH_EMPTY = 201;
	public static int ERROR_NOT_ENOUGH_NONEMPTY = 202;
	
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
	@SuppressWarnings("unused")
	private String getGoalJAXB() { return ((Enum<?>) goal.getValue()).name(); }
	@SuppressWarnings("unused")
	private void setGoalJAXB(String input) { goal.setFromString(input); }
	
	@XmlElement(name="count",required=true)
	private IntegerValidationParamNode count = new IntegerValidationParamNode(1);
	
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		final DataRecordMetadata inputMetadata = inMetadata;
		goal.setName("Count");
		count.setName("Minimal count");
		trimInput.setName("Trim input");
		trimInput.setTooltip("Trim input before validation.");
		trimInput.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				String[] targetField = ValidatorUtils.parseTargets(target.getValue());
				DataFieldMetadata fieldMetadata;
				for(int i = 0; i < targetField.length; i++) {
					fieldMetadata = inputMetadata.getField(targetField[i]);
					if(fieldMetadata != null && fieldMetadata.getDataType() == DataFieldType.STRING) {
						return true;
					}	
				}
				return false;
			}
		});
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(goal);
		parametersContainer.add(count);
		parametersContainer.add(trimInput);
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		setPropertyRefResolver(graphWrapper);
		logParams(StringUtils.mapToString(getProcessedParams(record.getMetadata(), graphWrapper), "=", "\n"));
		
		String[] targetField = ValidatorUtils.parseTargets(target.getValue());
		HashMap<String, String> values = new HashMap<String, String>();
		DataField field;
		int ok = 0;
		String inputString = null;
		for(int i = 0; i < targetField.length; i++) {
			field = record.getField(targetField[i]);
			inputString = field.toString();
			values.put(targetField[i], inputString);
			// Special treatment for strings as they can be filled with whitespaces
			if(field.getMetadata().getDataType() == DataFieldType.STRING) {
				if(trimInput.getValue()) {
					inputString = inputString.trim();
				}
				if(goal.getValue() == GOALS.EMPTY && inputString.isEmpty()) {
					ok++;	
				}
				if(goal.getValue() == GOALS.NONEMPTY && !inputString.isEmpty()) {
					ok++;
				}
			} else if(goal.getValue() == GOALS.EMPTY && field.isNull() ||
					goal.getValue() == GOALS.NONEMPTY && !field.isNull()) {
				ok++;
			}
			if(ok >= count.getValue()) {
				if(goal.getValue() == GOALS.EMPTY) {
					logSuccess(ok + " fields of required " + count.getValue() + " empty");
				} else {
					logSuccess(ok + " fields of required " + count.getValue() + " nonempty");
				}
				return State.VALID;
			}
		}
		// Error reporting
		if (ea != null) {
			if(goal.getValue() == GOALS.EMPTY) {
				raiseError(ea, ERROR_NOT_ENOUGH_NONEMPTY, "Only " + ok + " field(s) empty, " + count.getValue() +" empty field(s) required." , targetField, values);
			} else {
				raiseError(ea, ERROR_NOT_ENOUGH_EMPTY, "Only " + ok + " field(s) nonempty, " + count.getValue() +" nonempty field(s) required.", targetField, values);
			}
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
		if(count.getValue() == null || count.getValue().compareTo(Integer.valueOf(0)) <= 0) {
			accumulator.addError(count, this, "Count of fields must be greater than zero.");
			state = false;
		}
		if(!ValidatorUtils.areValidFields(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, "Some of target fields are not present in input metadata.");
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
	 * @return Param node with wanted count
	 */
	public IntegerValidationParamNode getCount() {
		return count;
	}
	/**
	 * @return Param node with current settings of trimming
	 */
	public BooleanValidationParamNode getTrimInput() {
		return trimInput;
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

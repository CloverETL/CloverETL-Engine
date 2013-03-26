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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.AbstractValidationRule.TARGET_TYPE;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="stringLength")
@XmlType(propOrder={"typeJAXB", "from", "to"})
public class StringLengthValidationRule extends StringValidationRule {
	public static enum TYPES {
		EXACT, MINIMAL, MAXIMAL, INTERVAL;
		@Override
		public String toString() {
			if(this.equals(EXACT)) {
				return "Exact";
			}
			if(this.equals(MINIMAL)) {
				return "Minimal";
			}
			if(this.equals(MAXIMAL)) {
				return "Maximal";
			}
			return "Interval";
		}
	}
	
	private EnumValidationParamNode type = new EnumValidationParamNode(TYPES.values(), TYPES.EXACT);
	@XmlElement(name="type", required=true)
	private String getTypeJAXB() { return ((Enum<?>) type.getValue()).name(); }
	private void setTypeJAXB(String input) { this.type.setFromString(input); }
	
	@XmlElement(name="from",required=true)
	private IntegerValidationParamNode from = new IntegerValidationParamNode();
	@XmlElement(name="to",required=true)
	private IntegerValidationParamNode to = new IntegerValidationParamNode();
	
	public List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		type.setName("Criterion");
		params.add(type);
		from.setPlaceholder("Not set");
		from.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				if(type.getValue() == TYPES.MAXIMAL) {
					return false;
				}
				return true;
			}
		});
		from.setName("From");
		params.add(from);
		to.setPlaceholder("Not set");
		to.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				if(type.getValue() == TYPES.MINIMAL || type.getValue() == TYPES.EXACT) {
					return false;
				}
				return true;
			}
		});
		to.setName("To");
		params.add(to);
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
				+ "Type: " + type.getValue() + "\n"
				+ "Lower bound: " + from.getValue() + "\n"
				+ "Upper bound: " + to.getValue() + "\n"
				+ "Trim input: " + trimInput.getValue());
		
		Integer length = Integer.valueOf(prepareInput(record.getField(target.getValue())).length());
		State result = State.INVALID;
		if(type.getValue() == TYPES.EXACT && length.equals(from.getValue())) {
			result = State.VALID;
		} else
		if(type.getValue() == TYPES.MAXIMAL && length.compareTo(to.getValue()) <= 0) {
			result = State.VALID;
		} else
		if(type.getValue() == TYPES.MINIMAL && length.compareTo(from.getValue()) >= 0) {
			result = State.VALID;
		} else
		if(type.getValue() == TYPES.INTERVAL && length.compareTo(from.getValue()) >= 0 && length.compareTo(to.getValue()) <= 0) {
			result = State.VALID;
		}
		logger.trace("Validation rule: " + getName() + " on length '" + length + "' is " + result);
		return result;
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
		if((type.getValue() == TYPES.EXACT || type.getValue() == TYPES.MINIMAL || type.getValue() == TYPES.INTERVAL) && from.getValue() == null) {
			accumulator.addError(from, this, "Parameter From is unset.");
			state = false;
		}
		if((type.getValue() == TYPES.MAXIMAL || type.getValue() == TYPES.INTERVAL) && to.getValue() == null) {
			accumulator.addError(to, this, "Parameter To is unset.");
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
	 * @return the type
	 */
	public EnumValidationParamNode getType() {
		return type;
	}
	/**
	 * @return the from
	 */
	public IntegerValidationParamNode getFrom() {
		return from;
	}
	/**
	 * @return the to
	 */
	public IntegerValidationParamNode getTo() {
		return to;
	}
	
	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}
	@Override
	public String getCommonName() {
		return "String Length";
	}
	@Override
	public String getCommonDescription() {
		return "Checks whether length of chosen field is of certain length.";
	}

}

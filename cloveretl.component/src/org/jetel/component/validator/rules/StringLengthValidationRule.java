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

import java.util.HashMap;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.data.DataRecord;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="stringLength")
public class StringLengthValidationRule extends StringValidationRule {
	public final static int TYPE = 100;
	public static enum TYPES {
		EXACT, MINIMAL, MAXIMAL, INTERVAL
	}
	public final static int FROM = 101;
	public final static int TO = 102;
	
	@XmlElement(name="target",required=true)
	private StringValidationParamNode target = new StringValidationParamNode(TARGET, "Target field");
	
	@XmlElement(name="type",required=true)
	private EnumValidationParamNode type;
	@XmlElement(name="from",required=true)
	private IntegerValidationParamNode from = new IntegerValidationParamNode(FROM, "From");
	@XmlElement(name="to",required=true)
	private IntegerValidationParamNode to = new IntegerValidationParamNode(TO, "To");
	
	public StringLengthValidationRule() {
		super();
		
		HashMap<Object, String> temp = new HashMap<Object, String>();
		temp.put(TYPES.EXACT, "Exact");
		temp.put(TYPES.MINIMAL, "At least");
		temp.put(TYPES.MAXIMAL, "At most");
		temp.put(TYPES.INTERVAL, "In interval");
		type = new EnumValidationParamNode(TYPE, "Criterion", temp, TYPES.EXACT);
		addParamNode(target);
		addParamNode(type);
		addParamNode(from);
		addParamNode(to);
	}
	

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
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
	public boolean isReady() {
		return !target.getValue().isEmpty() &&
				(
						(type.getValue() == TYPES.EXACT && from.getValue() != null) ||
						(type.getValue() == TYPES.MAXIMAL && to.getValue() != null) ||
						(type.getValue() == TYPES.MINIMAL && from.getValue() != null) ||
						(type.getValue() == TYPES.INTERVAL && from.getValue() != null && to.getValue() != null)
				);
	}

}

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

import java.util.Comparator;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.utils.comparators.DecimalComparator;
import org.jetel.component.validator.utils.comparators.DoubleComparator;
import org.jetel.component.validator.utils.comparators.LongComparator;
import org.jetel.component.validator.utils.comparators.StringComparator;
import org.jetel.component.validator.utils.convertors.Converter;
import org.jetel.component.validator.utils.convertors.DecimalConverter;
import org.jetel.component.validator.utils.convertors.DoubleConverter;
import org.jetel.component.validator.utils.convertors.LongConverter;
import org.jetel.component.validator.utils.convertors.StringConverter;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldType;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 8.1.2013
 */
@XmlRootElement(name="rangeCheck")
public class RangeCheckValidationRule extends AbstractValidationRule {
	
	// TYPE: Comparasion
	//  + Operator: ==, <=, >=, <, >, !=
	//  + Value:
	// TYPE: Interval
	//  + Boundaries: [), (], (), []
	//  + From:
	//  + To:
	
	public final static int TYPE = 1;
	public static enum TYPES {
		COMPARISON, INTERVAL
	};
	public final static int OPERATOR = 2;
	public static enum OPERATOR_TYPE {
		LE, GE, E, NE, L, G
	};
	public final static int BOUNDARIES = 3;
	public static enum BOUNDARIES_TYPE {
		OPEN_CLOSED, CLOSED_OPEN, OPEN_OPEN, CLOSED_CLOSED;
	};
	public final static int VALUE = 4;
	public final static int FROM = 5;
	public final static int TO = 6;
	public final static int TYPE_TO_USE = 7;
	public static enum METADATA_TYPES {
		DEFAULT, STRING
	}
	
//	public final static String USE_TYPE_METADATA = "use_type_metadata";
	
	@XmlElement(name="target",required=true)
	private StringValidationParamNode target = new StringValidationParamNode(TARGET, "Target field");
	
	@XmlElement(name="type",required=true)
	private EnumValidationParamNode type;
	
	@XmlElement(name="operator")
	private EnumValidationParamNode operator;
	@XmlElement(name="value")
	private StringValidationParamNode value = new StringValidationParamNode(VALUE, "Value");
	
	
	@XmlElement(name="boundaries")
	private EnumValidationParamNode boundaries;
	@XmlElement(name="from")
	private StringValidationParamNode from = new StringValidationParamNode(FROM, "From");
	@XmlElement(name="to")
	private StringValidationParamNode to = new StringValidationParamNode(TO, "To");
	
	@XmlElement(name="useType")
	private EnumValidationParamNode useType;
	
	
	public RangeCheckValidationRule() {
		super();
		HashMap<Object, String> temp = new HashMap<Object, String>();
		temp.put(TYPES.COMPARISON, "Comparison");
		temp.put(TYPES.INTERVAL, "Interval");
		type = new EnumValidationParamNode(TYPE, "Type", temp, TYPES.COMPARISON);
		
		temp = new HashMap<Object, String>();
		temp.put(OPERATOR_TYPE.E, "==");
		temp.put(OPERATOR_TYPE.NE, "!=");
		temp.put(OPERATOR_TYPE.GE, ">=");
		temp.put(OPERATOR_TYPE.LE, "<=");
		temp.put(OPERATOR_TYPE.L, "<");
		temp.put(OPERATOR_TYPE.G, ">");
		operator = new EnumValidationParamNode(OPERATOR, "Operator", temp, OPERATOR_TYPE.E);
		
		temp = new HashMap<Object, String>();
		temp.put(BOUNDARIES_TYPE.CLOSED_CLOSED, "[]");
		temp.put(BOUNDARIES_TYPE.CLOSED_OPEN, "[)");
		temp.put(BOUNDARIES_TYPE.OPEN_CLOSED, "(]");
		temp.put(BOUNDARIES_TYPE.OPEN_OPEN, "()");
		boundaries = new EnumValidationParamNode(BOUNDARIES, "Boundaries", temp, BOUNDARIES_TYPE.CLOSED_CLOSED);
		
		temp = new HashMap<Object, String>();
		temp.put(METADATA_TYPES.DEFAULT, "Use from metadata");
		useType = new EnumValidationParamNode(TYPE_TO_USE, "Type for comparison", temp, METADATA_TYPES.DEFAULT);
		
		addParamNode(target);
		addParamNode(type);
		addParamNode(operator);
		addParamNode(value);
		addParamNode(boundaries);
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
				+ "Target fields: " + target.getValue() + "\n"
				+ "Type: " + type.getValue() + "\n"
				+ " Comparison parameters: \n"
				+ "  Operator: " + operator.getValue() + "\n"
				+ "  Value: " + value.getValue() + "\n"
				+ " Interval parameters: \n"
				+ "  Boundaries: " + boundaries.getValue() + "\n"
				+ "  From: " + from.getValue() + "\n"
				+ "  To: " + to.getValue() + "\n");
		
		DataField field = record.getField(target.getValue());
		DataFieldType fieldType = field.getMetadata().getDataType();
		if(useType.getValue() != METADATA_TYPES.DEFAULT) {
			if(useType.getValue() == METADATA_TYPES.STRING) {
				fieldType = DataFieldType.STRING;
			}
		}
		State status = null;
		if (fieldType == DataFieldType.STRING) {
			System.out.println("Validation rule: " + getName() + ": Comparing as strings");
			status = checkInType(field, StringConverter.getInstance(), StringComparator.getInstance());
		} else if (fieldType == DataFieldType.INTEGER
				|| fieldType == DataFieldType.LONG
				|| fieldType == DataFieldType.DATE
				|| fieldType == DataFieldType.BYTE
				|| fieldType == DataFieldType.CBYTE
				|| fieldType == DataFieldType.BOOLEAN) {
			System.out.println("Validation rule: " + getName() + ": Comparing as longs");
			status = checkInType(field, LongConverter.getInstance(), LongComparator.getInstance());
		} else if (fieldType == DataFieldType.NUMBER) {
			System.out.println("Validation rule: " + getName() + ": Comparing as numbers");
			status = checkInType(field, DoubleConverter.getInstance(), DoubleComparator.getInstance());
		} else if (fieldType == DataFieldType.DECIMAL) {
			System.out.println("Validation rule: " + getName() + ": Comparing as decimals");
			status = checkInType(field, DecimalConverter.getInstance(), DecimalComparator.getInstance());
		} else {
			System.out.println("Validation rule: " + getName() + ": No comparing, unknown data type");
		}
		
		if(status == State.VALID) {
			logger.trace("Validation rule: " + getName() + " is " + State.VALID);
			return State.VALID;
		} else {
			logger.trace("Validation rule: " + getName() + " is " + State.INVALID);
			return State.INVALID;
		}
	}
	
	private <T> State checkInType(DataField dataField, Converter converter, Comparator<T> comparator) {
		T record = converter.convert(dataField.getValue());
		if(record == null) {
			return State.INVALID;
		}
		if(this.type.getValue() == TYPES.COMPARISON) {
			final OPERATOR_TYPE operator = (OPERATOR_TYPE) this.operator.getValue();
			T value = converter.convert(this.value.getValue());
			if(value == null) {
				return State.INVALID;
			}
			if(operator == OPERATOR_TYPE.E && comparator.compare(record, value) == 0) {
				return State.VALID;
			} else
			if(operator == OPERATOR_TYPE.NE && comparator.compare(record, value) != 0) {
				return State.VALID;
			} else
			if(operator == OPERATOR_TYPE.G && comparator.compare(record, value) > 0) {
				return State.VALID;
			} else
			if(operator == OPERATOR_TYPE.L && comparator.compare(record, value) < 0) {
				return State.VALID;
			} else
			if(operator == OPERATOR_TYPE.LE && comparator.compare(record, value) <= 0) {
				return State.VALID;
			} else
			if(operator == OPERATOR_TYPE.GE && comparator.compare(record, value) >= 0) {
				return State.VALID;
			} else {
				return State.INVALID;
			}
		} else {
			final BOUNDARIES_TYPE boundaries = (BOUNDARIES_TYPE) this.boundaries.getValue();
			T from = converter.convert(this.from.getValue());
			T to = converter.convert(this.to.getValue());
			if(from == null || to == null) {
				return State.INVALID;
			}
			if(boundaries == BOUNDARIES_TYPE.CLOSED_CLOSED && comparator.compare(record, from) >= 0 && comparator.compare(record, to) <= 0) {
				return State.VALID;
			} else
			if(boundaries == BOUNDARIES_TYPE.CLOSED_OPEN && comparator.compare(record, from) >= 0 && comparator.compare(record, to) < 0) {
				return State.VALID;
			} else
			if(boundaries == BOUNDARIES_TYPE.OPEN_CLOSED && comparator.compare(record, from) > 0 && comparator.compare(record, to) <= 0) {
				return State.VALID;
			} else
			if(boundaries == BOUNDARIES_TYPE.OPEN_OPEN && comparator.compare(record, from) > 0 && comparator.compare(record, to) < 0) {
				return State.VALID;
			} else {
				return State.INVALID;
			}
		}
	}
	
	@Override
	public boolean isReady() {
		// TODO Auto-generated method stub
		return false;
	}

}
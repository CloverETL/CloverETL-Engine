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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.AbstractValidationRule.TARGET_TYPE;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidatorUtils;
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
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 8.1.2013
 */
@XmlRootElement(name="interval")
@XmlType(propOrder={"boundariesJAXB", "from", "to", "useTypeJAXB" })
public class IntervalValidationRule extends AbstractValidationRule {
	
	// TYPE: Interval
	//  + Boundaries: [), (], (), []
	//  + From:
	//  + To:
	
	public static enum BOUNDARIES_TYPE {
		OPEN_CLOSED, CLOSED_OPEN, OPEN_OPEN, CLOSED_CLOSED;
		@Override
		public String toString() {
			if(this.equals(OPEN_CLOSED)) {
				return "(]";
			}
			if(this.equals(CLOSED_OPEN)) {
				return "[)";
			}
			if(this.equals(OPEN_OPEN)) {
				return "()";
			}
			return "[]";
		}
	};
	public static enum METADATA_TYPES {
		DEFAULT, STRING, LONG, NUMBER, DECIMAL;
		@Override
		public String toString() {
			if(this.equals(DEFAULT)) {
				return "Use from metadata";
			}
			if(this.equals(STRING)) {
				return "As string";
			}
			if(this.equals(LONG)) {
				return "As long";
			}
			if(this.equals(NUMBER)) {
				return "As number";
			}
			return "Decimal";
		}
	}
	
	private EnumValidationParamNode boundaries = new EnumValidationParamNode(BOUNDARIES_TYPE.values(), BOUNDARIES_TYPE.CLOSED_CLOSED);
	@XmlElement(name="boundaries")
	private String getBoundariesJAXB() { return ((Enum<?>) boundaries.getValue()).name(); }
	private void setBoundariesJAXB(String input) { this.boundaries.setFromString(input); }
	
	@XmlElement(name="from")
	private StringValidationParamNode from = new StringValidationParamNode();
	@XmlElement(name="to")
	private StringValidationParamNode to = new StringValidationParamNode();
	
	private EnumValidationParamNode useType = new EnumValidationParamNode(METADATA_TYPES.values(), METADATA_TYPES.DEFAULT);
	@XmlElement(name="useType")
	private String getUseTypeJAXB() { return ((Enum<?>) useType.getValue()).name(); }
	private void setUseTypeJAXB(String input) { this.useType.setFromString(input); }
	
	
	protected List<ValidationParamNode> initialize() {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		boundaries.setName("Boundaries");
		params.add(boundaries);
		from.setName("From");
		from.setPlaceholder("Not set");
		params.add(from);
		to.setName("To");
		to.setPlaceholder("Not set");
		params.add(to);
		useType.setName("Use type");
		params.add(useType);
		return params;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		logger.trace("Validation rule: " + this.getName() + "\n"
				+ "Target fields: " + target.getValue() + "\n"
				+ "  Boundaries: " + boundaries.getValue() + "\n"
				+ "  From: " + from.getValue() + "\n"
				+ "  To: " + to.getValue() + "\n");
		//FIXME: usedType
		
		DataField field = record.getField(target.getValue());
		DataFieldType fieldType = field.getMetadata().getDataType();
		if(useType.getValue() == METADATA_TYPES.STRING) {
			fieldType = DataFieldType.STRING;
		} else if(useType.getValue() == METADATA_TYPES.LONG) {
			fieldType = DataFieldType.LONG;
		} else if(useType.getValue() == METADATA_TYPES.NUMBER) {
			fieldType = DataFieldType.NUMBER;
		} else if(useType.getValue() == METADATA_TYPES.DECIMAL) {
			fieldType = DataFieldType.DECIMAL;
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
		final BOUNDARIES_TYPE boundaries = (BOUNDARIES_TYPE) this.boundaries.getValue();
		T from = converter.convert(this.from.getValue());
		T to = converter.convert(this.to.getValue());
		if(from == null || to == null) {
			return State.INVALID;
		}
		if(boundaries == BOUNDARIES_TYPE.CLOSED_CLOSED && comparator.compare(record, from) >= 0 && comparator.compare(record, to) <= 0) {
			return State.VALID;
		} else if(boundaries == BOUNDARIES_TYPE.CLOSED_OPEN && comparator.compare(record, from) >= 0 && comparator.compare(record, to) < 0) {
			return State.VALID;
		} else if(boundaries == BOUNDARIES_TYPE.OPEN_CLOSED && comparator.compare(record, from) > 0 && comparator.compare(record, to) <= 0) {
			return State.VALID;
		} else if(boundaries == BOUNDARIES_TYPE.OPEN_OPEN && comparator.compare(record, from) > 0 && comparator.compare(record, to) < 0) {
			return State.VALID;
		} else {
			return State.INVALID;
		}
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
		if(from.getValue().isEmpty()) {
			accumulator.addError(from, this, "Value From is empty.");
			state = false;
		}
		if(to.getValue().isEmpty()) {
			accumulator.addError(to, this, "Value To is empty.");
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
	 * @return the boundaries
	 */
	public EnumValidationParamNode getBoundaries() {
		return boundaries;
	}


	/**
	 * @return the from
	 */
	public StringValidationParamNode getFrom() {
		return from;
	}


	/**
	 * @return the to
	 */
	public StringValidationParamNode getTo() {
		return to;
	}


	/**
	 * @return the useType
	 */
	public EnumValidationParamNode getUseType() {
		return useType;
	}
	
	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}
	@Override
	public String getCommonName() {
		return "Interval";
	}
	@Override
	public String getCommonDescription() {
		return "Checks whether value of chosen field is in provided interval.";
	}

}
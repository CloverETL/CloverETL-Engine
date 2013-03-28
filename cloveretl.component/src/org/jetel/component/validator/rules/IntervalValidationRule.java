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
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.component.validator.utils.convertors.Converter;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 8.1.2013
 */
@XmlRootElement(name="interval")
@XmlType(propOrder={"boundariesJAXB", "from", "to"})
public class IntervalValidationRule extends ConversionValidationRule {
	
	// TYPE: Interval
	//  + Boundaries: [), (], (), []
	//  + From:
	//  + To:
	
	public static enum BOUNDARIES_TYPE {
		OPEN_CLOSED, CLOSED_OPEN, OPEN_OPEN, CLOSED_CLOSED;
		@Override
		public String toString() {
			if(this.equals(OPEN_CLOSED)) {
				return "Left open, right closed";
			}
			if(this.equals(CLOSED_OPEN)) {
				return "Left closed right open";
			}
			if(this.equals(OPEN_OPEN)) {
				return "Open";
			}
			return "Closed";
		}
	};
	
	private EnumValidationParamNode boundaries = new EnumValidationParamNode(BOUNDARIES_TYPE.values(), BOUNDARIES_TYPE.CLOSED_CLOSED);
	@XmlElement(name="boundaries")
	private String getBoundariesJAXB() { return ((Enum<?>) boundaries.getValue()).name(); }
	private void setBoundariesJAXB(String input) { this.boundaries.setFromString(input); }
	
	@XmlElement(name="from")
	private StringValidationParamNode from = new StringValidationParamNode();
	@XmlElement(name="to")
	private StringValidationParamNode to = new StringValidationParamNode();
	
	
	protected List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		boundaries.setName("Boundaries");
		params.add(boundaries);
		from.setName("From");
		from.setPlaceholder("Standard Clover format, for details see documentation.");
		params.add(from);
		to.setName("To");
		to.setPlaceholder("Not set");
		to.setPlaceholder("Standard Clover format, for details see documentation.");
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
				+ "Target fields: " + target.getValue() + "\n"
				+ "  Boundaries: " + boundaries.getValue() + "\n"
				+ "  From: " + from.getValue() + "\n"
				+ "  To: " + to.getValue() + "\n"
				+ "  Compare as: " + useType.getValue() + "\n"
				+ "  Format mask: " + format.getValue() + "\n"
				+ "  Locale: " + locale.getValue() + "\n"
				+ "  Timezone: " + timezone.getValue() + "\n"
				);
		
		DataField field = record.getField(target.getValue());
		DataFieldType fieldType = computeType(field);
		
		try {
			initConversionUtils(fieldType);
		} catch (IllegalArgumentException ex) {
			logger.trace("Validation rule: " + getName() + " is " + State.INVALID + " (cannot determine type to compare in)");
			return State.INVALID;
		}
		
		State status = checkInType(field, tempConverter, tempComparator);
		
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
		T from = converter.convertFromCloverLiteral(this.from.getValue());
		T to = converter.convertFromCloverLiteral(this.to.getValue());
		if(from == null || to == null) {
			return State.INVALID;
		}
		System.err.println("From: " + from);
		System.err.println("To:" + to);
		System.err.println("REcord: " + record);
		System.err.println("REcord vs from: " + comparator.compare(record, from));
		System.err.println("REcord vs to: " + comparator.compare(record, to));
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
		state &= super.isReady(inputMetadata, accumulator);
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
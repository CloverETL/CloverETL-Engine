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
import java.util.Locale;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.AbstractValidationRule.TARGET_TYPE;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.component.validator.utils.comparators.DateComparator;
import org.jetel.component.validator.utils.comparators.DecimalComparator;
import org.jetel.component.validator.utils.comparators.DoubleComparator;
import org.jetel.component.validator.utils.comparators.LongComparator;
import org.jetel.component.validator.utils.comparators.StringComparator;
import org.jetel.component.validator.utils.convertors.Converter;
import org.jetel.component.validator.utils.convertors.DateConverter;
import org.jetel.component.validator.utils.convertors.DecimalConverter;
import org.jetel.component.validator.utils.convertors.DoubleConverter;
import org.jetel.component.validator.utils.convertors.LongConverter;
import org.jetel.component.validator.utils.convertors.StringConverter;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 8.1.2013
 */
@XmlRootElement(name="comparison")
@XmlType(propOrder={"operatorJAXB", "value" })
public class ComparisonValidationRule extends ConversionValidationRule {
	
	// TYPE: Comparasion
	//  + Operator: ==, <=, >=, <, >, !=
	//  + Value:
	
	public static enum OPERATOR_TYPE {
		LE, GE, E, NE, L, G;
		@Override
		public String toString() {
			if(this.equals(LE)) {
				return "<=";
			}
			if(this.equals(GE)) {
				return ">=";
			}
			if(this.equals(E)) {
				return "==";
			}
			if(this.equals(NE)) {
				return "!=";
			}
			if(this.equals(L)) {
				return "<";
			}
			return ">";
		}
	};
	
	private EnumValidationParamNode operator = new EnumValidationParamNode(OPERATOR_TYPE.values(), OPERATOR_TYPE.E);
	@XmlElement(name="operator", required=true)
	private String getOperatorJAXB() { return ((Enum<?>) operator.getValue()).name(); }
	private void setOperatorJAXB(String input) { this.operator.setFromString(input); }
	
	@XmlElement(name="value")
	private StringValidationParamNode value = new StringValidationParamNode();
	
	protected List<ValidationParamNode> initialize() {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		operator.setName("Operator");
		params.add(operator);
		value.setName("Compare with");
		params.add(value);
		params.addAll(super.initialize());
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
				+ "  Operator: " + operator.getValue() + "\n"
				+ "  Value: " + value.getValue() + "\n"
				+ "  Compare as: " + useType.getValue() + "\n"
				+ "  Format mask: " + format.getValue() + "\n"
				+ "  Strict mode: " + strict.getValue() + "\n");
		
		DataField field = record.getField(target.getValue());
		DataFieldType fieldType = computeType(field);
		
		State status = null;
		if (fieldType == DataFieldType.STRING) {
			System.out.println("Validation rule: " + getName() + ": Comparing as strings");
			status = checkInType(field, StringConverter.getInstance(), StringComparator.getInstance());
		} else if (fieldType == DataFieldType.INTEGER
				|| fieldType == DataFieldType.LONG
				|| fieldType == DataFieldType.BYTE
				|| fieldType == DataFieldType.CBYTE
				|| fieldType == DataFieldType.BOOLEAN) {
			System.out.println("Validation rule: " + getName() + ": Comparing as longs");
			status = checkInType(field, LongConverter.getInstance(), LongComparator.getInstance());
		} else if (fieldType == DataFieldType.DATE) {
			String localeString = (record.getMetadata().getLocaleStr() == null) ? Defaults.DEFAULT_LOCALE : record.getField(target.getValue()).getMetadata().getLocaleStr(); 
			Locale locale = new Locale(localeString);
			// FIXME: store it for later
			DateConverter converter = DateConverter.newInstance(format.getValue(), strict.getValue(), locale);
			status = checkInType(field, converter, DateComparator.getInstance());
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

		final OPERATOR_TYPE operator = (OPERATOR_TYPE) this.operator.getValue();
		T value = converter.convert(this.value.getValue());
		if(value == null) {
			return State.INVALID;
		}
		if(operator == OPERATOR_TYPE.E && comparator.compare(record, value) == 0) {
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.NE && comparator.compare(record, value) != 0) {
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.G && comparator.compare(record, value) > 0) {
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.L && comparator.compare(record, value) < 0) {
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.LE && comparator.compare(record, value) <= 0) {
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.GE && comparator.compare(record, value) >= 0) {
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
		if(value.getValue().isEmpty()) {
			accumulator.addError(value, this, "Value to compare with is empty.");
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
	
	public EnumValidationParamNode getOperator() {
		return operator;
	}

	/**
	 * @return the value
	 */
	public StringValidationParamNode getValue() {
		return value;
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
		return "Comparison";
	}
	@Override
	public String getCommonDescription() {
		return "Checks whether value of chosen field satisfies comparison aganist provided value and operation.";
	}

}
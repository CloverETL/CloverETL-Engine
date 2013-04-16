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
import org.jetel.util.string.StringUtils;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 8.1.2013
 */
@XmlRootElement(name="comparison")
@XmlType(propOrder={"operatorJAXB", "value" })
public class ComparisonValidationRule extends ConversionValidationRule {
	
	public static final int ERROR_INIT_CONVERSION = 901;
	public static final int ERROR_FIELD_CONVERSION = 902;
	public static final int ERROR_VALUE_CONVERSION = 903;
	public static final int ERROR_CONDITION_NOT_MET = 904;
	
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
				return "=";
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
	@SuppressWarnings("unused")
	@XmlElement(name="operator", required=true)
	private String getOperatorJAXB() { return ((Enum<?>) operator.getValue()).name(); }
	@SuppressWarnings("unused")
	private void setOperatorJAXB(String input) { this.operator.setFromString(input); }
	
	@XmlElement(name="value")
	private StringValidationParamNode value = new StringValidationParamNode();
	
	protected List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		operator.setName("Operator");
		params.add(operator);
		value.setName("Compare with");
		value.setPlaceholder("Standard Clover format, for details see documentation.");
		params.add(value);
		params.addAll(super.initialize(inMetadata, graphWrapper));
		return params;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		setPropertyRefResolver(graphWrapper);
		logParams(StringUtils.mapToString(getProcessedParams(record.getMetadata(), graphWrapper), "=", "\n"));
		logParentLangaugeSetting();
		logLanguageSettings();
		
		String resolvedTarget = resolve(target.getValue());
		
		List<String> nodePath = graphWrapper.getNodePath(this);
		
		DataField field = record.getField(resolvedTarget);
		DataFieldType fieldType = computeType(field);
		try {
			initConversionUtils(fieldType);
		} catch (IllegalArgumentException ex) {
			raiseError(ea, ERROR_INIT_CONVERSION, "Cannot initialize conversion and comparator tools.", nodePath, resolvedTarget, field.getValue().toString());
			return State.INVALID;
		}
		
		if(field.isNull()) {
			logSuccess("Field '" + resolvedTarget + "' is null.");
			return State.VALID;
		}
		
		State status = checkInType(field, tempConverter, tempComparator, ea, nodePath);
		
		if(status == State.VALID) {
			return State.VALID;
		} else {
			return State.INVALID;
		}
	}
	
	private <T extends Object> State checkInType(DataField dataField, Converter converter, Comparator<T> comparator, ValidationErrorAccumulator ea, List<String> nodePath) {
		String resolvedTarget = resolve(target.getValue());
		String resolvedValue = resolve(value.getValue());
		
		T record = converter.<T>convert(dataField.getValue());
		if(record == null) {
			raiseError(ea, ERROR_FIELD_CONVERSION, "Conversion failed.", nodePath, resolvedTarget,(dataField.getValue() == null) ? "null" : dataField.getValue().toString());
			return State.INVALID;
		}

		final OPERATOR_TYPE operator = (OPERATOR_TYPE) this.operator.getValue();
		
		T value = converter.<T>convertFromCloverLiteral(resolvedValue);
		if(value == null) {
			raiseError(ea, ERROR_VALUE_CONVERSION, "Conversion of value failed.", nodePath, resolvedTarget,this.value.getValue());
			return State.INVALID;
		}
		if(operator == OPERATOR_TYPE.E && comparator.compare(record, value) == 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' = '" + value.toString() + "'.");
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.NE && comparator.compare(record, value) != 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' != '" + value.toString() + "'.");
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.G && comparator.compare(record, value) > 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' > '" + value.toString() + "'.");
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.L && comparator.compare(record, value) < 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' < '" + value.toString() + "'.");
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.LE && comparator.compare(record, value) <= 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' <= '" + value.toString() + "'.");
			return State.VALID;
		} else if(operator == OPERATOR_TYPE.GE && comparator.compare(record, value) >= 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' >= '" + value.toString() + "'.");
			return State.VALID;
		} else {
			raiseError(ea, ERROR_CONDITION_NOT_MET, "Incoming value did not meet the condition.", nodePath, resolvedTarget, dataField.getValue().toString());
			return State.INVALID;
		}
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		setPropertyRefResolver(graphWrapper);
		boolean state = true;
		String resolvedTarget = resolve(target.getValue());
		String resolvedValue = resolve(value.getValue());
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		if(resolvedValue.isEmpty()) {
			accumulator.addError(value, this, "Value to compare with is empty.");
			state = false;
		}
		state &= super.isReady(inputMetadata, accumulator, graphWrapper);
		return state;
	}
	
	public EnumValidationParamNode getOperator() {
		return operator;
	}
	public StringValidationParamNode getValue() {
		return value;
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
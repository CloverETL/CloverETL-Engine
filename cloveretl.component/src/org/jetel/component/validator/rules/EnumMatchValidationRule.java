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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.rules.ComparisonValidationRule.OPERATOR_TYPE;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.component.validator.utils.comparators.StringComparator;
import org.jetel.component.validator.utils.convertors.Converter;
import org.jetel.component.validator.utils.convertors.StringConverter;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="enumMatch")
@XmlType(propOrder={"values" , "ignoreCase", "trimInput"})
public class EnumMatchValidationRule extends ConversionValidationRule {

	@XmlElement(name="values",required=true)
	private StringValidationParamNode values = new StringValidationParamNode();
	@XmlElement(name="ignoreCase",required=false)
	private BooleanValidationParamNode ignoreCase = new BooleanValidationParamNode(false);
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	
	private Set<Object> tempValues;
	
	public List<ValidationParamNode> initialize() {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		values.setName("Accept values");
		values.setTooltip("For example:\nfirst,second\nfirst,\"second,third\",fourth");
		values.setPlaceholder("Comma separated list of values");
		params.add(values);
		params.addAll(super.initialize());
		ignoreCase.setName("Ignore case");
		params.add(ignoreCase);
		ignoreCase.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				if(useType.getValue() == METADATA_TYPES.STRING ) {
					return true;
				}
				return false;
			}
		});
		trimInput.setName("Trim input");
		params.add(trimInput);
		trimInput.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				if(useType.getValue() == METADATA_TYPES.STRING ) {
					return true;
				}
				return false;
			}
		});
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
					+ "Accepted values: " + values.getValue() + "\n"
					+ "Compare as: " + useType.getValue() + "\n"
					+ "Format mask: " + format.getValue() + "\n"
					+ "Locale: " + locale.getValue() + "\n"
					+ "Timezone: " + timezone.getValue() + "\n"
					+ "Ignoring case: " + ignoreCase.getValue() + "\n"
					+ "Trim input: " + trimInput.getValue());
		
		DataField field = record.getField(target.getValue());
		DataFieldType fieldType = computeType(field);
		try {
			initConversionUtils(fieldType);
		} catch (IllegalArgumentException ex) {
			logger.trace("Validation rule: " + getName() + " is " + State.INVALID + " (cannot determine type to compare in)");
			return State.INVALID;
		}
		
		try {
			getParsedValues();
		} catch (NullPointerException ex) {
			logger.trace("Validation rule: " + getName() + " is " + State.INVALID + " (cannot parse accepted values)");
			return State.INVALID;
		}
		
		State status = checkInType(field);
		
		if(status == State.VALID) {
			logger.trace("Validation rule: " + getName() + " is " + State.VALID);
			return State.VALID;
		} else {
			logger.trace("Validation rule: " + getName() + " is " + State.INVALID);
			return State.INVALID;
		}
	}
	
	private Set<Object> getParsedValues() {
		if(tempValues == null) {
			Set<String> values = parseValues(ignoreCase.getValue());
			Set<Object> out = new HashSet<Object>();
			for(String value : values) {
				Object temp = tempConverter.convertFromCloverLiteral(value);
				if(temp == null) {
					throw new NullPointerException("Cannot parse values;");
				}
				out.add(temp);
			}
			tempValues = out;
		}
		return tempValues;
	}
	
	private <T> State checkInType(DataField dataField) {
		T record = tempConverter.convert(dataField.getValue());
		if(record == null) {
			return State.INVALID;
		}
		
		Set<Object> temp = getParsedValues();
		for(Object item : temp) {
			if(tempComparator instanceof StringComparator) {
				if(trimInput.getValue()) {
					record = (T) ((String) record).trim();
				}
				if(ignoreCase.getValue()) {
					record = (T) ((String) record).toLowerCase();
					item = (T) ((String) item).toLowerCase();
				}
				System.err.println(record);
				System.err.println(item);
				if(tempComparator.compare(record, item) == 0) {
					return State.VALID;		
				}	
			} else
			if(tempComparator.compare(item, record) == 0) {
				return State.VALID;		
			}
		}
		return State.INVALID;
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
		if(parseValues(false).isEmpty()) {
			accumulator.addError(values, this, "No values for matching were provided.");
			state = false;
		}
		return state;
	}

	private Set<String> parseValues(boolean ignoreCase) {
		Set<String> temp;
		if(ignoreCase) {
			temp = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		} else {
			temp = new TreeSet<String>();
		}
		String[] temp2 = StringUtils.split(values.getValue(),",");
		stripDoubleQuotes(temp2);
		temp.addAll(Arrays.asList(temp2));
		// Workaround because split ignores "something,else," <-- last comma
		if(values.getValue().endsWith(",")) {
			temp.add(new String());
		}
		return temp;
	}
	private void stripDoubleQuotes(String[] input) {
		for(int i = 0; i < input.length; i++) {
			if(input[i].startsWith("\"") && input [i].endsWith("\"")) {
				input[i] = input[i].substring(1, input[i].length()-1);
			}
		}
	}
	
	/**
	 * @return the target
	 */
	public StringValidationParamNode getTarget() {
		return target;
	}

	/**
	 * @return the values
	 */
	public StringValidationParamNode getValues() {
		return values;
	}

	/**
	 * @return the ignoreCase
	 */
	public BooleanValidationParamNode getIgnoreCase() {
		return ignoreCase;
	}
	
	public BooleanValidationParamNode getTrimInput() {
		return trimInput;
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}

	@Override
	public String getCommonName() {
		return "Enum Match";
	}

	@Override
	public String getCommonDescription() {
		return "Checks whether chosen field contains value from enumeration.";
	}

}

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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.component.validator.utils.comparators.StringComparator;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * <p>Rule for checking whether incoming value is contained in enumeration provided by user.</p>
 * 
 * Available settings:
 * <ul>
 *  <li>Enumeration. All values in Clover format, delimited by commas.</li>
 *  <li>IgnoreCase: True/False.</li>
 *  <li>Trim input. If the input should be trimmed before processing.</li>
 * </ul>
 *  
 * <p>Uses language settings inherited from @link {@link ConversionValidationRule}.</p>
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 * @see ConversionValidationRule
 */
@XmlRootElement(name="enumMatch")
@XmlType(propOrder={"values" , "ignoreCase", "trimInput"})
public class EnumMatchValidationRule<T> extends ConversionValidationRule<T> {
	
	public static final int ERROR_INIT_CONVERSION = 701;	/** Converter and/or comparator could not been initialized */
	public static final int ERROR_PARSING_VALUES = 702;		/** Enumeration provided by user could not been parsed */
	public static final int ERROR_FIELD_CONVERSION = 703;	/** Conversion of incoming value failed */
	public static final int ERROR_NO_MATCH = 704;			/** Incoming value was not present in enumeration */

	@XmlElement(name="values",required=true)
	private StringValidationParamNode values = new StringValidationParamNode();
	@XmlElement(name="ignoreCase",required=false)
	private BooleanValidationParamNode ignoreCase = new BooleanValidationParamNode(false);
	@XmlElement(name="trimInput",required=false)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(false);
	
	private Set<T> tempValues;
	private String resolvedTarget;
	private int fieldPosition;
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		values.setName(ValidatorMessages.getString("EnumMatchValidationRule.AcceptedValuesParameterName")); //$NON-NLS-1$
		values.setTooltip(ValidatorMessages.getString("EnumMatchValidationRule.AcceptedValuesParameterTooltip")); //$NON-NLS-1$
		values.setPlaceholder(ValidatorMessages.getString("EnumMatchValidationRule.AcceptedValuesParameterPlaceholder")); //$NON-NLS-1$
		
		ignoreCase.setName(ValidatorMessages.getString("EnumMatchValidationRule.IgnoreCaseParameterName")); //$NON-NLS-1$
		ignoreCase.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				if(useType.getValue() == METADATA_TYPES.STRING ) {
					return true;
				}
				return false;
			}
		});
		trimInput.setName(ValidatorMessages.getString("EnumMatchValidationRule.TrimInputParameterName")); //$NON-NLS-1$
		trimInput.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				if(useType.getValue() == METADATA_TYPES.STRING ) {
					return true;
				}
				return false;
			}
		});
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(values);
		parametersContainer.add(ignoreCase);
		parametersContainer.add(trimInput);
	}
	
	@Override
	public void init(DataRecordMetadata metadata, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(metadata, graphWrapper);
		resolvedTarget = resolve(target.getValue());
		
		fieldPosition = metadata.getFieldPosition(resolvedTarget);
		DataFieldMetadata field = metadata.getField(fieldPosition);
		DataFieldType fieldType = computeType(field.getDataType());
		try {
			initConversionUtils(fieldType);
		} catch (IllegalArgumentException ex) {
			throw new ComponentNotReadyException("Cannot initialize conversion and comparator tools."); //$NON-NLS-1$
		}
		
		try {
			getParsedValues();
		} catch (NullPointerException ex) {
			throw new ComponentNotReadyException(ValidatorMessages.getString("EnumMatchValidationRule.EnumParseError")); //$NON-NLS-1$
		}

	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return State.NOT_VALIDATED;
		}
		
		DataField field = record.getField(fieldPosition);
		State status = checkInType(field, tempComparator, ea);
		
		if(status == State.VALID) {
			return State.VALID;
		} else {
			return State.INVALID;
		}
	}
	
	private Set<T> getParsedValues() {
		if(tempValues == null) {
			Set<String> values = parseValues(ignoreCase.getValue());
			Set<T> out = new HashSet<T>();
			for(String value : values) {
				T temp = tempConverter.convertFromCloverLiteral(value);
				if(temp == null) {
					throw new NullPointerException(ValidatorMessages.getString("EnumMatchValidationRule.ValuesParseError")); //$NON-NLS-1$
				}
				out.add(temp);
			}
			tempValues = out;
		}
		return tempValues;
	}
	
	private State checkInType(DataField dataField, Comparator<T> comparator, ValidationErrorAccumulator ea) {
		T record = tempConverter.<T>convert(dataField.getValue());
		if(record == null) {
			if (ea != null)
				raiseError(ea, ERROR_FIELD_CONVERSION, ValidatorMessages.getString("EnumMatchValidationRule.InputDataConversionError"), resolvedTarget,(dataField.getValue() == null) ?"null" : dataField.getValue().toString()); //$NON-NLS-1$ //$NON-NLS-2$
			return State.INVALID;
		}
		
		Set<T> temp = getParsedValues();
		String stringRecord;
		String stringItem;
		/// FIXME temp.contains(...) should be used instead
		for(T item : temp) {
			if(comparator instanceof StringComparator) {
				if(trimInput.getValue()) {
					stringRecord = ((String) record).trim();
				} else {
					stringRecord = (String) record;
				}
				stringItem = ((String) item);
				if(ignoreCase.getValue()) {
					stringRecord = stringRecord.toLowerCase();
					stringItem = stringItem.toLowerCase();
				}
				if(((StringComparator) comparator).compare(stringRecord, stringItem) == 0) {
					return State.VALID;
				}	
			} else {
				if(comparator.compare(item, record) == 0) {
					return State.VALID;
				}
			}
		}
		if (ea != null)
			raiseError(ea, ERROR_NO_MATCH, ValidatorMessages.getString("EnumMatchValidationRule.NoMatchErrorMessage"), resolvedTarget, record.toString()); //$NON-NLS-1$
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
			accumulator.addError(target, this, ValidatorMessages.getString("EnumMatchValidationRule.EmptyTargetFieldError")); //$NON-NLS-1$
			state = false;
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, ValidatorMessages.getString("EnumMatchValidationRule.12")); //$NON-NLS-1$
			state = false;
		}
		if(parseValues(false).isEmpty()) {
			accumulator.addError(values, this, ValidatorMessages.getString("EnumMatchValidationRule.13")); //$NON-NLS-1$
			state = false;
		}
		state &= super.isReady(inputMetadata, accumulator, graphWrapper);
		return state;
	}

	private Set<String> parseValues(boolean ignoreCase) {
		Set<String> temp;
		if(ignoreCase) {
			temp = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
		} else {
			temp = new TreeSet<String>();
		}
		String resolvedValues = resolve(values.getValue());
		String[] temp2 = StringUtils.split(resolvedValues,","); //$NON-NLS-1$
		stripDoubleQuotesAndTrim(temp2);
		temp.addAll(Arrays.asList(temp2));
		// Workaround because split ignores "something,else," <-- last comma
		if(resolvedValues.endsWith(",")) { //$NON-NLS-1$
			temp.add(new String());
		}
		return temp;
	}
	
	private void stripDoubleQuotesAndTrim(String[] input) {
		for(int i = 0; i < input.length; i++) {
			input[i] = input[i].trim();
			if(input[i].startsWith("\"") && input [i].endsWith("\"")) { //$NON-NLS-1$ //$NON-NLS-2$
				input[i] = input[i].substring(1, input[i].length()-1);
			}
		}
	}

	/**
	 * @return Param nodes with enumeration
	 */
	public StringValidationParamNode getValues() {
		return values;
	}

	/**
	 * @return Param node with ignore case settings
	 */
	public BooleanValidationParamNode getIgnoreCase() {
		return ignoreCase;
	}
	
	/**
	 * @return Param node with trim input settings
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
		return ValidatorMessages.getString("EnumMatchValidationRule.18"); //$NON-NLS-1$
	}

	@Override
	public String getCommonDescription() {
		return ValidatorMessages.getString("EnumMatchValidationRule.19"); //$NON-NLS-1$
	}

}

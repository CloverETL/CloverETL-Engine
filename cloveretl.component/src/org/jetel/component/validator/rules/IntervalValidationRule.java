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
import java.util.Comparator;

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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * <p>Rule for checking that incoming value is in interval provided by user.</p>
 * 
 * Available settings:
 * <ul>
 * 	<li>Boundaries. Type of boundaries @see {@link BOUNDARIES_TYPE}</li>
 *  <li>Left boundary.</li>
 *  <li>Right boundary.</li>
 * </ul>
 * 
 * <p>Uses language settings inherited from @link {@link ConversionValidationRule}.</p>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 8.1.2013
 */
@XmlRootElement(name="interval")
@XmlType(propOrder={"boundariesJAXB", "from", "to"})
public class IntervalValidationRule extends ConversionValidationRule {
	
	public static final int ERROR_INIT_CONVERSION = 801;	/** Initialization of converter and comparator failed */
	public static final int ERROR_FIELD_CONVERSION = 802;	/** Converting of incoming field failed */
	public static final int ERROR_FROM_CONVERSION = 803;	/** Converting of left boundary from user failed */
	public static final int ERROR_TO_CONVERSION = 804;		/** Converting of right boundary from user failed */
	public static final int ERROR_NOT_IN_INTERVAL = 805;	/** Incoming value was not in interval */
	
	/**
	 * Types of interval boundaries
	 */
	public static enum BOUNDARIES_TYPE {
		OPEN_CLOSED, CLOSED_OPEN, OPEN_OPEN, CLOSED_CLOSED;
		@Override
		public String toString() {
			if(this.equals(OPEN_CLOSED)) {
				return "From inclusive, to exclusive";
			}
			if(this.equals(CLOSED_OPEN)) {
				return "From exclusive, to inclusive";
			}
			if(this.equals(OPEN_OPEN)) {
				return "From exclusive, to exclusive";
			}
			return "From inclusive, to inclusive";
		}
	};
	
	private EnumValidationParamNode boundaries = new EnumValidationParamNode(BOUNDARIES_TYPE.values(), BOUNDARIES_TYPE.CLOSED_CLOSED);
	@XmlElement(name="boundaries")
	@SuppressWarnings("unused")
	private String getBoundariesJAXB() { return ((Enum<?>) boundaries.getValue()).name(); }
	@SuppressWarnings("unused")
	private void setBoundariesJAXB(String input) { this.boundaries.setFromString(input); }
	
	@XmlElement(name="from")
	private StringValidationParamNode from = new StringValidationParamNode();
	@XmlElement(name="to")
	private StringValidationParamNode to = new StringValidationParamNode();
	private String resolvedTarget;
	private int fieldPosition;
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		boundaries.setName("Boundaries");
		from.setName("From");
		from.setPlaceholder("Standard Clover format, for details see documentation.");
		to.setName("To");
		to.setPlaceholder("Not set");
		to.setPlaceholder("Standard Clover format, for details see documentation.");
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(boundaries);
		parametersContainer.add(from);
		parametersContainer.add(to);
	}
	
	@Override
	public void init(DataRecordMetadata metadata, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(metadata, graphWrapper);
		
		setPropertyRefResolver(graphWrapper);
		logParams(StringUtils.mapToString(getProcessedParams(metadata, graphWrapper), "=", "\n"));
		logParentLangaugeSetting();
		logLanguageSettings();
		
		resolvedTarget = resolve(target.getValue());
		fieldPosition = metadata.getFieldPosition(resolvedTarget);
		
		DataFieldType fieldType = computeType(metadata.getDataFieldType(fieldPosition));

		try {
			initConversionUtils(fieldType);
		} catch (IllegalArgumentException ex) {
			throw new ComponentNotReadyException("Cannot initialize conversion and comparator tools.", ex);
		}
	}
	

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logNotValidated("Rule is not enabled.");
			return State.NOT_VALIDATED;
		}
		
		DataField field = record.getField(fieldPosition);
		
		// Null values are valid by definition
		if(field.isNull()) {
			if (isLoggingEnabled()) {
				logSuccess("Field '" + resolvedTarget + "' is null.");
			}
			return State.VALID;
		}
		
		State status = checkInType(field, tempConverter, tempComparator, ea);
		
		if(status == State.VALID) {
			return State.VALID;
		} else {
			return State.INVALID;
		}
	}
	
	private <T extends Object> State checkInType(DataField dataField, Converter converter, Comparator<T> comparator, ValidationErrorAccumulator ea) {
		String resolvedTarget = resolve(target.getValue());
		String resolvedTo = resolve(to.getValue());
		String resolvedFrom = resolve(from.getValue());
		
		T record = converter.<T>convert(dataField.getValue());
		if(record == null) {
			// FIXME: remove unused check for null
			if (ea != null)
				raiseError(ea, ERROR_FIELD_CONVERSION, "Conversion of value from record failed.", resolvedTarget,(dataField.getValue() == null) ? "null" : dataField.getValue().toString());
			return State.INVALID;
		}
		final BOUNDARIES_TYPE boundaries = (BOUNDARIES_TYPE) this.boundaries.getValue();
		T from = converter.<T>convertFromCloverLiteral(resolvedFrom);
		T to = converter.<T>convertFromCloverLiteral(resolvedTo);
		if(from == null) {
			if (ea != null)
				raiseError(ea, ERROR_FROM_CONVERSION, "Conversion of value 'From' failed.", resolvedTarget,this.from.getValue());
		}
		if(to == null) {
			if (ea != null)
				raiseError(ea, ERROR_TO_CONVERSION, "Conversion of value 'To' failed.", resolvedTarget,this.to.getValue());
		}
		if(from == null || to == null) {
			return State.INVALID;
		}
		if(boundaries == BOUNDARIES_TYPE.CLOSED_CLOSED && comparator.compare(record, from) >= 0 && comparator.compare(record, to) <= 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' is in interval ['" + from.toString() + "', '" + to.toString() + "'].");
			return State.VALID;
		} else if(boundaries == BOUNDARIES_TYPE.CLOSED_OPEN && comparator.compare(record, from) >= 0 && comparator.compare(record, to) < 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' is in interval ['" + from.toString() + "', '" + to.toString() + "').");
			return State.VALID;
		} else if(boundaries == BOUNDARIES_TYPE.OPEN_CLOSED && comparator.compare(record, from) > 0 && comparator.compare(record, to) <= 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' is in interval ('" + from.toString() + "', '" + to.toString() + "'].");
			return State.VALID;
		} else if(boundaries == BOUNDARIES_TYPE.OPEN_OPEN && comparator.compare(record, from) > 0 && comparator.compare(record, to) < 0) {
			logSuccess("Field '" + resolvedTarget + "' with value '" + record.toString() + "' is in interval ('" + from.toString() + "', '" + to.toString() + "').");
			return State.VALID;
		} else {
			if (ea != null) {
				raiseError(ea, ERROR_NOT_IN_INTERVAL, "Incoming value not in given interval.", resolvedTarget, record.toString());
			}
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
		String resolvedTo = resolve(to.getValue());
		String resolvedFrom = resolve(from.getValue());
		
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		if(resolvedFrom.isEmpty()) {
			accumulator.addError(from, this, "Value From is empty.");
			state = false;
		}
		if(resolvedTo.isEmpty()) {
			accumulator.addError(to, this, "Value To is empty.");
			state = false;
		}
		state &= super.isReady(inputMetadata, accumulator, graphWrapper);
		return state;
	}

	/**
	 * @return Param node with boundaries
	 */
	public EnumValidationParamNode getBoundaries() {
		return boundaries;
	}


	/**
	 * @return Param node with left boundary of interval
	 */
	public StringValidationParamNode getFrom() {
		return from;
	}


	/**
	 * @return Param node with right boundary of interval
	 */
	public StringValidationParamNode getTo() {
		return to;
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
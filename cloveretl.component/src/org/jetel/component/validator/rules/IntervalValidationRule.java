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
import org.jetel.component.validator.ValidatorMessages;
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
public class IntervalValidationRule<T> extends ConversionValidationRule<T> {
	
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
				return ValidatorMessages.getString("IntervalValidationRule.BoundaryOpenClosed"); //$NON-NLS-1$
			}
			if(this.equals(CLOSED_OPEN)) {
				return ValidatorMessages.getString("IntervalValidationRule.BoundaryClosedOpen"); //$NON-NLS-1$
			}
			if(this.equals(OPEN_OPEN)) {
				return ValidatorMessages.getString("IntervalValidationRule.BoundaryOpenOpen"); //$NON-NLS-1$
			}
			return ValidatorMessages.getString("IntervalValidationRule.BoundaryClosedClosed"); //$NON-NLS-1$
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
	private T fromTyped;
	private T toTyped;
	private String resolvedTo;
	private String resolvedFrom;
	private BOUNDARIES_TYPE boundariesValue;
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		boundaries.setName(ValidatorMessages.getString("IntervalValidationRule.BoundariesParameterName")); //$NON-NLS-1$
		from.setName(ValidatorMessages.getString("IntervalValidationRule.FromParameterName")); //$NON-NLS-1$
		from.setPlaceholder(ValidatorMessages.getString("IntervalValidationRule.FromParameterPlaceholder")); //$NON-NLS-1$
		to.setName(ValidatorMessages.getString("IntervalValidationRule.ToParameterName")); //$NON-NLS-1$
		to.setPlaceholder(ValidatorMessages.getString("IntervalValidationRule.ToParameterPlaceholder")); //$NON-NLS-1$
		to.setPlaceholder(ValidatorMessages.getString("IntervalValidationRule.ToParameterPlaceholder2")); //$NON-NLS-1$
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
		
		resolvedTarget = resolve(target.getValue());
		fieldPosition = metadata.getFieldPosition(resolvedTarget);
		
		DataFieldType fieldType = computeType(metadata.getDataFieldType(fieldPosition));

		try {
			initConversionUtils(fieldType);
		} catch (IllegalArgumentException ex) {
			throw new ComponentNotReadyException(ValidatorMessages.getString("IntervalValidationRule.InitErrorInternalConversionUtilsInitFail"), ex); //$NON-NLS-1$
		}
		
		resolvedTo = resolve(to.getValue());
		resolvedFrom = resolve(from.getValue());
		
		boundariesValue = (BOUNDARIES_TYPE) this.boundaries.getValue();
		fromTyped = tempConverter.<T>convertFromCloverLiteral(resolvedFrom);
		toTyped = tempConverter.<T>convertFromCloverLiteral(resolvedTo);
		if(fromTyped == null) {
			throw new ComponentNotReadyException(ValidatorMessages.getString("IntervalValidationRule.InitErrorFromInvalid")); //$NON-NLS-1$
		}
		if(toTyped == null) {
			throw new ComponentNotReadyException(ValidatorMessages.getString("IntervalValidationRule.InitErrorToInvalid")); //$NON-NLS-1$
		}
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return State.NOT_VALIDATED;
		}
		
		DataField field = record.getField(fieldPosition);
		
		// Null values are valid by definition
		// XXX really? why?
		if(field.isNull()) {
			return State.VALID;
		}
		
		State status = checkInType(field, tempConverter, tempComparator, ea);
		
		if(status == State.VALID) {
			return State.VALID;
		} else {
			return State.INVALID;
		}
	}
	
	private State checkInType(DataField dataField, Converter converter, Comparator<T> comparator, ValidationErrorAccumulator ea) {
		T incomingValue = converter.<T>convert(dataField.getValue());
		if(incomingValue == null) {
			// FIXME: remove unused check for null
			if (ea != null)
				raiseError(ea, ERROR_FIELD_CONVERSION, ValidatorMessages.getString("IntervalValidationRule.InvalidRecordMessageConversionFail"), resolvedTarget,(dataField.getValue() == null) ? "null" : dataField.getValue().toString()); //$NON-NLS-1$ //$NON-NLS-2$
			return State.INVALID;
		}
		
		State state;
		if(boundariesValue == BOUNDARIES_TYPE.CLOSED_CLOSED && comparator.compare(incomingValue, fromTyped) >= 0 && comparator.compare(incomingValue, toTyped) <= 0) {
			state = State.VALID;
		} else if(boundariesValue == BOUNDARIES_TYPE.CLOSED_OPEN && comparator.compare(incomingValue, fromTyped) >= 0 && comparator.compare(incomingValue, toTyped) < 0) {
			state = State.VALID;
		} else if(boundariesValue == BOUNDARIES_TYPE.OPEN_CLOSED && comparator.compare(incomingValue, fromTyped) > 0 && comparator.compare(incomingValue, toTyped) <= 0) {
			state = State.VALID;
		} else if(boundariesValue == BOUNDARIES_TYPE.OPEN_OPEN && comparator.compare(incomingValue, fromTyped) > 0 && comparator.compare(incomingValue, toTyped) < 0) {
			state = State.VALID;
		} else {
			if (ea != null) {
				raiseError(ea, ERROR_NOT_IN_INTERVAL, ValidatorMessages.getString("IntervalValidationRule.InvalidRecordMessageNotInInterval"), resolvedTarget, incomingValue.toString()); //$NON-NLS-1$
			}
			state = State.INVALID;
		}
		
		return state;
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		String resolvedTarget = resolve(target.getValue());
		String resolvedTo = resolve(to.getValue());
		String resolvedFrom = resolve(from.getValue());
		
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, ValidatorMessages.getString("IntervalValidationRule.ConfigurationErrorTargetEmpty")); //$NON-NLS-1$
			state = false;
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, ValidatorMessages.getString("IntervalValidationRule.ConfigurationErrorTargetMissing")); //$NON-NLS-1$
			state = false;
		}
		if(resolvedFrom.isEmpty()) {
			accumulator.addError(from, this, ValidatorMessages.getString("IntervalValidationRule.ConfigurationErrorFromIsEmpty")); //$NON-NLS-1$
			state = false;
		}
		if(resolvedTo.isEmpty()) {
			accumulator.addError(to, this, ValidatorMessages.getString("IntervalValidationRule.ConfigurationErrorToIsEmpty")); //$NON-NLS-1$
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
		return ValidatorMessages.getString("IntervalValidationRule.CommonName"); //$NON-NLS-1$
	}
	@Override
	public String getCommonDescription() {
		return ValidatorMessages.getString("IntervalValidationRule.CommonDescription"); //$NON-NLS-1$
	}

}
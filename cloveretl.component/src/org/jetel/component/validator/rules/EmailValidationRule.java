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

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Rule that check email address conformity to RFC 822 using the javax.mail package.
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29.5.2013
 */
@XmlRootElement(name="email")
@XmlType(propOrder={"plainAddressParam", "allowGroupAddressesParam"})
public class EmailValidationRule extends StringValidationRule {
	
	public static final int INVALID_EMAIL_ADDRESS = 1301;
	public static final int NOT_PLAIN_EMAIL_ADDRESS = 1302;
	public static final int GROUP_EMAIL_ADDRESS = 1303;
	public static final int EMPTY_EMAIL_ADDRESS = 1304;

	@XmlElement(name="plainAddress",required=false)
	private BooleanValidationParamNode plainAddressParam = new BooleanValidationParamNode(false);
	@XmlElement(name="allowGroupAddresses",required=false)
	private BooleanValidationParamNode allowGroupAddressesParam = new BooleanValidationParamNode(false);
	
	@Override
	public void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		plainAddressParam.setName(ValidatorMessages.getString("EmailValidationRule.PlainAddressParameterName")); //$NON-NLS-1$
		allowGroupAddressesParam.setName(ValidatorMessages.getString("EmailValidationRule.GroupAddressesParameterName")); //$NON-NLS-1$
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(plainAddressParam);
		parametersContainer.add(allowGroupAddressesParam);
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}
	
	@Override
	public void init(DataRecordMetadata metadata, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(metadata, graphWrapper);
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return State.NOT_VALIDATED;
		}
		
		String inputString = prepareInput(record);
		
		if (inputString == null || inputString.isEmpty()) {
			if (ea != null)
				raiseError(ea, EMPTY_EMAIL_ADDRESS, ValidatorMessages.getString("EmailValidationRule.EmptyStringError"), resolvedTarget, inputString); //$NON-NLS-1$
			return State.INVALID;
		}
		
		boolean plainAddress = plainAddressParam.getValue();
		boolean allowGroupAddresses = allowGroupAddressesParam.getValue();
		ValidationError validationResult = validate(inputString, plainAddress, allowGroupAddresses);
		if (validationResult != null) {
			if (ea != null)
				raiseError(ea, validationResult.getErrorCode(), validationResult.getErrorMessage(), resolvedTarget, inputString);
			return State.INVALID;
		}
		else {
			if (isLoggingEnabled()) {
			}
			return State.VALID;
		}
	}
	
	private static final class ValidationError {
		private final int errorCode;
		private final String errorMessage;
		public ValidationError(int errorCode, String errorMessage) {
			this.errorCode = errorCode;
			this.errorMessage = errorMessage;
		}
		public int getErrorCode() {
			return errorCode;
		}
		public String getErrorMessage() {
			return errorMessage;
		}
	}

	private ValidationError validate(String inputString, boolean plainAddress, boolean allowGroupAddresses) {
		try {
			InternetAddress internetAddress = new InternetAddress(inputString);
			internetAddress.validate();
			if (!allowGroupAddresses && internetAddress.isGroup()) {
				return new ValidationError(GROUP_EMAIL_ADDRESS, ValidatorMessages.getString("EmailValidationRule.GroupAddressError")); //$NON-NLS-1$
			}
			if (plainAddress && !inputString.equals(internetAddress.getAddress())) {
				return new ValidationError(NOT_PLAIN_EMAIL_ADDRESS, ValidatorMessages.getString("EmailValidationRule.NotPlainEmailAddress")); //$NON-NLS-1$
			}
		} catch (AddressException e) {
			return new ValidationError(INVALID_EMAIL_ADDRESS, e.getMessage());
		}
		return null;
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator,
			GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		String resolvedTarget = target.getValue();
		boolean state = true;
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, ValidatorMessages.getString("EmailValidationRule.TargetEmpty")); //$NON-NLS-1$
			state = false;
		}
		DataFieldMetadata field = inputMetadata.getField(resolvedTarget);
		if(field == null) { 
			accumulator.addError(target, this, ValidatorMessages.getString("EmailValidationRule.TargetFieldMissing")); //$NON-NLS-1$
			state = false;
		}
		else {
			if (field.getDataType() != DataFieldType.STRING) {
				accumulator.addError(target, this, ValidatorMessages.getString("EmailValidationRule.TargetFieldNotStringError")); //$NON-NLS-1$
				state = false;
			}
		}
		state &= super.isReady(inputMetadata, accumulator, graphWrapper);
		return state;
	}

	@Override
	public String getCommonName() {
		return ValidatorMessages.getString("EmailValidationRule.CommonName"); //$NON-NLS-1$
	}

	@Override
	public String getCommonDescription() {
		return ValidatorMessages.getString("EmailValidationRule.CommonDescription"); //$NON-NLS-1$
	}
	
	public BooleanValidationParamNode getPlainAddressParam() {
		return plainAddressParam;
	}
	
	public BooleanValidationParamNode getAllowGroupAddressesParam() {
		return allowGroupAddressesParam;
	}

}

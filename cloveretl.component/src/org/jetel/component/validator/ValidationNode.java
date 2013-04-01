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
package org.jetel.component.validator;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Abstract shared part of each validation rule or group.
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
@XmlAccessorType(XmlAccessType.NONE)
public abstract class ValidationNode {
	protected final static Log logger = LogFactory.getLog(ValidationNode.class);
	
	@XmlAttribute(required=true)
	private boolean enabled = true;
	@XmlAttribute(required=false)
	private String name;
	
	/**
	 * Result states of validation
	 */
	public enum State {
		VALID, NOT_VALIDATED, INVALID;
	}
	
	public ValidationNode() {
		setName(getCommonName());
	}
	
	/**
	 * Validates given record against the rule.
	 * @param record Record to be validated
	 * @param ea Error accumulator where all errors are stored, can be null
	 * @param graphWrapper Object which holds graph instance
	 * @return Not null validation state, NOT_VALIDATED when rule is disabled or group not entered
	 */
	public abstract State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper);
	
	/**
	 * Return whether the rule parameters are valid and therefore is ready to validate.
	 * Always not lazy to obtain all errors.
	 * @param inputMetadata Input metadata from graph, used for checking if target fields are present
	 * @param accumulator Error accumulator in which all errors with human readable messages 
	 * @return true if parameters are valid, false otherwise
	 */
	public abstract boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator);
	
	public void logSuccess(String message) {
		logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' is " + State.VALID + ": " + message);
	}
	
	public void logNotValidated(String message) {
		logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' is " + State.NOT_VALIDATED + ": " + message);
	}
	
	public void logError(String message) {
		logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' is " + State.INVALID + ": " + message);
	}
	
	public void logParams(String params) {
		logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' has parameters:\n" + params);
	}
	
	/**
	 * @return Returns true when rule is enabled 
	 */
	public boolean isEnabled() {
		return enabled;
	}
	/**
	 * Sets whether the rule is enabled/disabled
	 * @param enabled
	 */
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	/**
	 * @return Returns current name
	 */
	public String getName() {
		return name;
	}
	/**
	 * Sets name of rule
	 * @param name New name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Returns common name of rule type. Used for default name of new rules.  
	 * @return Name
	 */
	public abstract String getCommonName();
	/**
	 * Returns common description of rule type
	 * @return Description
	 */
	public abstract String getCommonDescription();
}

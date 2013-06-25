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
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.rules.LanguageSettingsValidationRule;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.PropertyRefResolver;

/**
 * Abstract class with shared functionality of validation rules or group.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 * @see GraphWrapper
 * @see LanguageSetting
 * @see LanguageSetting#hierarchicMerge(LanguageSetting, LanguageSetting)
 * @see AbstractValidationRule#raiseError(ValidationErrorAccumulator, int, String, java.util.List, java.util.List, java.util.Map)
 * @see ValidationError
 */
@XmlAccessorType(XmlAccessType.NONE)
public abstract class ValidationNode {
	/**
	 * Result states of validation
	 */
	public enum State {
		VALID, NOT_VALIDATED, INVALID;
	}
	
	protected final static Log logger = LogFactory.getLog(ValidationNode.class);
	
	@XmlAttribute(required=true)
	private boolean enabled = true;
	@XmlAttribute(required=false)
	private String name;
	
	protected LanguageSetting parentLanguageSetting;
	
	private PropertyRefResolver refResolver;
	
	private boolean initialized;
	
	/**
	 * Class constructor which sets default name of rule.
	 */
	public ValidationNode() {
		setName(getCommonName());
	}
	
	public void init(DataRecord record, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		initialized = true;
		refResolver = graphWrapper.getRefResolver();
	}

	public boolean isInitialized() {
		return initialized;
	}
	
	public void preExecute() {
	}
	
	public void postExecute() {
	}
	
	/**
	 * Validates given record against the rule.
	 * 
	 * Tutorial for implementing:
	 * <ul>
	 *   <li>Add new class in annotation of class {@link AbstractValidationRule} (without it, (de)serialization will fail)</li>
	 *   <li>Check if rule is enabled</li>
	 *   <li>Log rule parameters {@link #logParams(String)}<br />
	 *    Log parent language settings {@link #logParentLangaugeSetting()}<br />
	 *    When extending {@link LanguageSettingsValidationRule} log rule language settings {@link LanguageSettingsValidationRule#logLanguageSettings()}</li>
	 *   <li>Set property solver from graph when resolving (CTL code) should be used ({@link #setPropertyRefResolver(GraphWrapper)}</li>
	 *   <li>Resolve string variables {@link #resolve(String)}</li>
	 *   <li>For using inherited locale/timezone/format masks use {@link LanguageSetting#hierarchicMerge(LanguageSetting, LanguageSetting)}</li>
	 *   <li>Log tracing reports {@link #logSuccess(String)} {@link #logError(String)} {@link #logNotValidated(String)} or generic {@link #logger}</li>
	 *   <li>Always return state (never NULL), when returning INVALID always {@link AbstractValidationRule#raiseError} otherwise the rule will behave inconsistently</li>
	 * </ul> 
	 *
	 * Have in mind:
	 * <ul>
	 *   <li>When unexpected happen INVALID should be returned</li>
	 *   <li>{@link #isValid(DataRecord, ValidationErrorAccumulator, GraphWrapper)} is not invoked if {@link #isReady(DataRecordMetadata, ReadynessErrorAcumulator, GraphWrapper)} is false</li>
	 * </ul>
	 *  
	 * @param record Record to be validated
	 * @param ea Error accumulator where all errors are stored, can be null
	 * @param graphWrapper Object which holds graph instance
	 * @return Not null validation state, NOT_VALIDATED when rule is disabled or group not entered
	 */
	public abstract State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper);
	
	/**
	 * Return whether the rule parameters are valid and therefore is ready to validate.
	 * Always not lazy to obtain all errors.
	 * 
	 * Have in mind:
	 * <ul>
	 *   <li>Not enabled rules are always ready and return true.</li>
	 *   <li>When this method returns true rule should have everything ready for validation (check empty, formats, target fields)</li>
	 *   <li>Set property solver from graph when resolving (CTL code) should be used ({@link #setPropertyRefResolver(GraphWrapper)}</li>
	 *   <li>Resolve string variables {@link #resolve(String)}</li>
	 *   <li>When using {@link LanguageSettingsValidationRule} always add errors on param node from original language settings not the merged one<br />
	 *    (Otherwise no warning sign will appear in GUI)</li>
	 * </ul>
	 * @param inputMetadata Input metadata from graph, used for checking if target fields are present
	 * @param accumulator Error accumulator in which all errors with human readable messages 
	 * @return true if parameters are valid, false otherwise
	 */
	public abstract boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper);
	
	/**
	 * Log that rule ended with VALID state
	 * @param message Description of output point from rule
	 */
	public void logSuccess(String message) {
		if (logger.isTraceEnabled()) {
			logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' is " + State.VALID + ": " + message);
		}
	}
	
	/**
	 * Log that rule ended with NOT_VALIDATED state
	 * @param message Description of output point from rule
	 */
	public void logNotValidated(String message) {
		if (logger.isTraceEnabled()) {
			logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' is " + State.NOT_VALIDATED + ": " + message);
		}
	}
	
	/**
	 * Log that rule ended with INVALID state
	 * @param message Description of output point from rule
	 */
	public void logError(String message) {
		if (logger.isTraceEnabled()) {
			logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' is " + State.INVALID + ": " + message);
		}
	}
	
	/**
	 * Log all parameters of rule
	 * @param message Serialized parameters
	 */
	public void logParams(String params) {
		if (logger.isTraceEnabled()) {
			logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' has parameters:\n" + params);
		}
	}
	
	/**
	 * Log language setting of parent
	 */
	public void logParentLangaugeSetting() {
		if (logger.isTraceEnabled()) {
			logger.trace("Node '" + (getName().isEmpty() ? getCommonName() : getName()) + "' has parent language setting:\n" + parentLanguageSetting);
		}
	}
	
	public boolean isLoggingEnabled() {
		return logger.isTraceEnabled();
	}
	
	
	/**
	 * @return Returns true when rule is enabled, false otherwise
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
	 * Sets name of rule shown in GUI and error reporting
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
	 * Returns common description of rule type shown in list of all rules
	 * @return Common description of rule type
	 */
	public abstract String getCommonDescription();
	
	/**
	 * Sets property resolver for replacing variables in parameters
	 * @param graphWrapper Graph Wrapper from which the resolver is obtained
	 */
	public void setPropertyRefResolver(GraphWrapper graphWrapper) {
		if(graphWrapper != null) {
			this.refResolver = graphWrapper.getRefResolver();	
		}
	}
	/**
	 * Resolves given string
	 * @param input String to resolve
	 * @return Resolved string without variables
	 */
	protected String resolve(String input) {
		if(refResolver == null) {
			throw new RuntimeException("Cannot resolve variable, no resolver given. Validation rule is implemented wrong (call setPropertyRefResolver before resolve).");
		}
		return refResolver.resolveRef(input);
	}
	
	/**
	 * Injects parents language setting
	 * @param languageSetting Parent language setting
	 */
	public void setParentLanguageSetting(LanguageSetting languageSetting) {
		parentLanguageSetting = languageSetting;
	}
	/**
	 * Returns parent language setting
	 * @return Parent language setting
	 */
	public LanguageSetting getParentLanguageSetting() {
		return parentLanguageSetting;
	}
}

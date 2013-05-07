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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.rules.ComparisonValidationRule;
import org.jetel.component.validator.rules.CustomValidationRule;
import org.jetel.component.validator.rules.DateValidationRule;
import org.jetel.component.validator.rules.EnumMatchValidationRule;
import org.jetel.component.validator.rules.IntervalValidationRule;
import org.jetel.component.validator.rules.LookupValidationRule;
import org.jetel.component.validator.rules.NonEmptyFieldValidationRule;
import org.jetel.component.validator.rules.NonEmptySubsetValidationRule;
import org.jetel.component.validator.rules.NumberValidationRule;
import org.jetel.component.validator.rules.PatternMatchValidationRule;
import org.jetel.component.validator.rules.StringLengthValidationRule;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Class with shared functionality for all validation rules.
 * It provide parameter where target field(s) are stored (divided by comma)
 * ALWAYS ADD REFERENCE TO A NEW VALIDATION RULES TO THIS ANNOTATION!
 * (Otherwise serialization and deserialization will not work.)
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
@XmlSeeAlso({
		ValidationGroup.class,
		EnumMatchValidationRule.class,
		NonEmptyFieldValidationRule.class, 
		NonEmptySubsetValidationRule.class, 
		PatternMatchValidationRule.class, 
		StringLengthValidationRule.class,
		IntervalValidationRule.class,
		ComparisonValidationRule.class,
		DateValidationRule.class,
		NumberValidationRule.class,
		LookupValidationRule.class,
		CustomValidationRule.class
	})
public abstract class AbstractValidationRule extends ValidationNode {
	
	private Map<String, String> tempParams;
	
	/**
	 * Types of targets:
	 *  - ONE_FIELD - rule works with only one field
	 *  - UNORDERED_FIELDS - rule works with multiple field but order does not matter
	 *  - ORDERED_FIELDS - rule works with multiple field, the order matters
	 *  @see AbstractValidationRule#getTargetType()
	 */
	public static enum TARGET_TYPE {
		ONE_FIELD, UNORDERED_FIELDS, ORDERED_FIELDS
	}
	
	@XmlElement(name="target",required=true)
	protected StringValidationParamNode target = new StringValidationParamNode();
	
	private List<ValidationParamNode> params;
	
	/**
	 * Returns lazy initialized param nodes for GUI. Intended to be used in GUI.
	 * 
	 * @param inMetadata Metadata of incoming record
	 * @param graphWrapper Graph wrapper to be able finish initialization and reach some graph parameters
	 * @return List of all param nodes
	 */
	public List<ValidationParamNode> getParamNodes(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		if(params == null) {
			params = initialize(inMetadata, graphWrapper);
		}
		return params;
	}
	
	/**
	 * Returns lazy initialized param nodes in map mostly for debugging reason. Intended to be used in engine.
	 * 
	 * Call this before {@link #getProcessedParams()}!
	 * @param inMetadata Metadata of incoming record
	 * @param graphWrapper Graph wrapper to be able finish initialization and reach some graph parameters.
	 * @return Map of all param nodes and its values
	 */
	public Map<String, String> getProcessedParams(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		if(tempParams == null) {
			Map<String, String> temp = new HashMap<String, String>();
			// Shared params
			temp.put("Targets", getTarget().getValue());
			List<ValidationParamNode> paramNodes = getParamNodes(inMetadata, graphWrapper);
			for(ValidationParamNode paramNode : paramNodes) {
				temp.put(paramNode.getName(), paramNode.toString());
			}
			tempParams = temp;
		}
		return tempParams;
	}
	/**
	 * Returns lazy initialized param nodes in map mostly for debugging reason. Intended to be used in engine.
	 * 
	 * Call after {@link #getProcessedParams(DataRecordMetadata, GraphWrapper)}
	 * @return Map of all param nodes and its values
	 */
	public Map<String, String> getProcessedParams() {
		return tempParams;
	}
	
	/**
	 * Initialize param nodes.
	 * Have in mind:
	 * <ul>
	 *   <li>Always return not null</li>
	 * </ul>
	 * @param inMetadata Metadata incoming record
	 * @param graphWrapper Graph wrapper to be able finish initialization and reach some graph parameters
	 * @return List of all param nodes
	 */
	protected abstract List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper);
	
	public StringValidationParamNode getTarget() {
		return target;
	}
	
	/**
	 * Returns target type of validation rule
	 * @return Not null target type
	 */
	public abstract TARGET_TYPE getTargetType();
	
	
	/**
	 * Creates new error and append it into error accumulator if given.
	 * 
	 * @param accumulator Error accumulator to which the error is added
	 * @param code Code of error
	 * @param message Human readable reason of error
	 * @param fields List of fields on which the error has happened
	 * @param values Map of field and values
	 */
	public void raiseError(ValidationErrorAccumulator accumulator, int code, String message, List<String> path, List<String> fields, Map<String, String> values) {
		if(accumulator == null) {
			return;
		}
		ValidationError error = new ValidationError();
		if(getName().isEmpty()) {
			error.setName(getCommonName());	
		} else {
			error.setName(getName());
		}
		error.setPath(path);
		error.setCode(code);
		error.setMessage(message);
		error.setFields(fields);
		error.setValues(values);
		error.setParams(getProcessedParams());
		
		accumulator.addError(error);
		logError(message);
		logger.trace(error);
	}
	
	/** @see #raiseError(ValidationErrorAccumulator, int, String, String, List, Map) */
	public void raiseError(ValidationErrorAccumulator accumulator, int code, String message, List<String> path, String[] fields, Map<String, String> values) {
		raiseError(accumulator, code, message, path, Arrays.asList(fields), values);
	}
	
	/** @see #raiseError(ValidationErrorAccumulator, int, String, String, List, Map) */
	public void raiseError(ValidationErrorAccumulator accumulator, int code, String message, List<String> path, String field, String value) {
		HashMap<String, String> temp = new HashMap<String, String>();
		temp.put(field, value);
		raiseError(accumulator, code, message, path, Arrays.asList(field), temp);
	}
}

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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * <p>Rule for checking whether given fields fulfils given regular expression</p>
 * 
 * Available settings:
 * <ul>
 * 	<li>Pattern: Java regexp.</li>
 *  <li>IgnoreCase: True/False.</li>
 * </ul>
 * 
 * <p>For other settings etc. see @link {@link StringValidationRule}</p>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="patternMatch")
@XmlType(propOrder={"ignoreCase", "pattern"})
public class PatternMatchValidationRule extends StringValidationRule {
	
	public static final int ERROR_INVALID_PATTERN = 601;
	public static final int ERROR_NO_MATCH = 602;
	
	@XmlElement(name="ignoreCase",required=true)
	private BooleanValidationParamNode ignoreCase = new BooleanValidationParamNode(false);
	@XmlElement(name="pattern",required=true)
	private StringValidationParamNode pattern = new StringValidationParamNode();
	private Pattern regexPattern;
	
	@Override
	protected void initializeParameters(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		super.initializeParameters(inMetadata, graphWrapper);
		
		pattern.setName("Pattern to match");
		pattern.setPlaceholder("Regular expression, for syntax see documentation");
		ignoreCase.setName("Ignore case");
	}
	
	@Override
	protected void registerParameters(Collection<ValidationParamNode> parametersContainer) {
		super.registerParameters(parametersContainer);
		
		parametersContainer.add(pattern);
		parametersContainer.add(ignoreCase);
	}
	
	@Override
	public void init(DataRecordMetadata metadata, GraphWrapper graphWrapper) throws ComponentNotReadyException {
		super.init(metadata, graphWrapper);
		
		String resolvedPattern = resolve(pattern.getValue());
		
		try {
			if(ignoreCase.getValue()) {
				regexPattern = Pattern.compile(resolvedPattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
			} else {
				regexPattern = Pattern.compile(resolvedPattern, Pattern.UNICODE_CASE);
			}
		} catch (PatternSyntaxException e) {
			throw new ComponentNotReadyException("Invalid pattern specified", e);
		}
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return State.NOT_VALIDATED;
		}
		
		String tempString = null;
		tempString = prepareInput(record);

		if(regexPattern.matcher(tempString).matches()) {
			return State.VALID;
		} else {
			if (ea != null)
				raiseError(ea, ERROR_NO_MATCH, "No match.", resolvedTarget, tempString);
			return State.INVALID;
		}
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		String resolvedTarget = resolve(target.getValue());
		String resolvedPattern = resolve(pattern.getValue());
		if(resolvedTarget.isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(resolvedPattern.isEmpty()) {
			accumulator.addError(pattern, this, "Match pattern is empty.");
			state = false;
		}
		if(!ValidatorUtils.isValidField(resolvedTarget, inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		state &= super.isReady(inputMetadata, accumulator, graphWrapper);
		return state;
	}

	/**
	 * @return Param node with current settings of case sensitivity
	 */
	public BooleanValidationParamNode getIgnoreCase() {
		return ignoreCase;
	}


	/**
	 * @return Param node with regexp
	 */
	public StringValidationParamNode getPattern() {
		return pattern;
	}
	
	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}


	@Override
	public String getCommonName() {
		return "Pattern Match";
	}


	@Override
	public String getCommonDescription() {
		return "Checks whether chosen field matches regular expression provided by user.";
	}

}

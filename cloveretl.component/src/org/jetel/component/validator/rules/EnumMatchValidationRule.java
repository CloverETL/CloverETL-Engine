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
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.data.DataRecord;
import org.jetel.util.string.StringUtils;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
@XmlRootElement(name="enumMatch")
public class EnumMatchValidationRule extends StringValidationRule {
	public final static int IGNORE_CASE = 100;
	public final static int VALUES = 101;
	
	@XmlElement(name="target",required=true)
	private StringValidationParamNode target = new StringValidationParamNode(TARGET, "Target field");
	@XmlElement(name="values",required=true)
	private StringValidationParamNode values = new StringValidationParamNode(VALUES, "Accept values");
	@XmlElement(name="ignoreCase",required=true)
	private BooleanValidationParamNode ignoreCase = new BooleanValidationParamNode(IGNORE_CASE, "Ingore case", false);
	public EnumMatchValidationRule() {
		super();
		addParamNode(target);
		addParamNode(values);
		addParamNode(ignoreCase);
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		logger.trace("Validation rule: " + this.getName() + "\n"
					+ "Target field: " + target.getValue() + "\n"
					+ "Accepted values: " + values.getValue() + "\n"
					+ "Ignoring case: " + ignoreCase.getValue() + "\n"
					+ "Trim input: " + trimInput.getValue());
		
		String tempString = prepareInput(record.getField(target.getValue()));
		Set<String> values = parseValues(ignoreCase.getValue());
		if(values.contains(tempString)) {
			logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.VALID);
			return State.VALID;
		} else {
			// TODO: error reporting
			logger.trace("Validation rule: " + getName() + "  on '" + tempString + "' is " + State.INVALID);
			return State.INVALID;
		}
	}

	@Override
	public boolean isReady() {
		return !target.getValue().isEmpty() &&
				!parseValues(false).isEmpty();
				
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

}

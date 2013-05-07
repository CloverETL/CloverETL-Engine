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

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jetel.component.Validator;
import org.jetel.util.string.StringUtils;

/**
 * Carries information about error from validation rule to reporting.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 20.11.2012
 * @see ValidationErrorAccumulator
 * @see ValidationNode#isValid(org.jetel.data.DataRecord, ValidationErrorAccumulator, GraphWrapper)
 * @see Validator#createErrorOutputMetadata()
 */
public class ValidationError {
	private int code;
	private String message;
	private String name;
	private List<String> path;
	private List<String> fields;
	private Map<String,String> values;
	private Map<String,String> params;
	private Date timestamp;
	
	public ValidationError() {
		timestamp = new Date();
	}

	/**
	 * @return Code of error
	 */
	public int getCode() {
		return code;
	}

	/**
	 * @return Message of error
	 */
	public String getMessage() {
		return message;
	}

	/**
	 * @return Name of rule
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * @return Path to the validation node which failed
	 */
	public List<String> getPath() {
		return path;
	}

	/**
	 * @return Fields on which the validation was performed
	 */
	public List<String> getFields() {
		return fields;
	}
	
	/**
	 * @return Fields on which the validation was performed in string
	 */
	public String getFieldsInString() {
		if(fields == null) {
			return null;
		}
		return StringUtils.stringArraytoString(fields.toArray(new String[]{}),",");
	}

	/**
	 * @return Values on which the validation was performed
	 */
	public Map<String, String> getValues() {
		return values;
	}
	
	/**
	 * @return Values on which the validation was performed in string
	 */
	public String getValuesInString() {
		if(values == null) {
			return null;
		}
		return StringUtils.mapToString(values, "=", ",");
	}

	/**
	 * @return All params on validation node on which the validation was performed
	 */
	public Map<String, String> getParams() {
		return params;
	}
	/**
	 * @return All params on validation node on which the validation was performed in string
	 */
	public String getParamsInString() {
		if(params == null) {
			return null;
		}
		return StringUtils.mapToString(params, "=", ",");
	}

	/**
	 * @return Timestamp when validation error happened
	 */
	public Date getTimestamp() {
		return timestamp;
	}

	/**
	 * @param code the code to set
	 */
	public void setCode(int code) {
		this.code = code;
	}

	/**
	 * @param message the message to set
	 */
	public void setMessage(String message) {
		this.message = message;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * @param path the path to set
	 */
	public void setPath(List<String> path) {
		this.path = path;
	}

	/**
	 * @param fields the fields to set
	 */
	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	/**
	 * @param values the values to set
	 */
	public void setValues(Map<String, String> values) {
		this.values = values;
	}

	/**
	 * @param params the params to set
	 */
	public void setParams(Map<String, String> params) {
		this.params = params;
	}
	
	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Validation error\n");
		buffer.append("----------------\n");
		buffer.append("Name: " + name + "\n");
		buffer.append("Path: " + path + "\n");
		buffer.append("Timestamp: " + timestamp.toString() + "\n");
		buffer.append("Code: " + code + "\n");
		buffer.append("Message: " + message + "\n");
		buffer.append("Fields: " + getFieldsInString() + "\n");
		buffer.append("Values: " + getValuesInString() + "\n");
		buffer.append("Params: " + getParamsInString() + "\n");
		return buffer.toString();
	}
	
	
}

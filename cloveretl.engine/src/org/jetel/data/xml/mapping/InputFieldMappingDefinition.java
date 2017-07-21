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
package org.jetel.data.xml.mapping;



/**  Represents a mapping from input field to an output field as a part of the XML to record mapping.
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.6.2012
 */
public class InputFieldMappingDefinition extends XMLMappingDefinition {

	/**
	 * 	Represents a field, where the sequence number should be stored.
	 */
	private String inputField;
	
	/**
	 * 	Represents a sequence ID to be used.
	 */
	private String outputField;

	/**
	 * Creates a new instance of the element to record mapping as root.
	 */
	public InputFieldMappingDefinition() {
		super();
	}

	/**
	 * Creates a new instance of the element to record mapping with given parent.
	 * 
	 * @param parent
	 */
	public InputFieldMappingDefinition(XMLMappingDefinition parent) {
		super(parent);
	}

	/** Copies an attributes of this mapping to a given mapping.
	 * 
	 * @param mapping
	 */
	public void copyTo(InputFieldMappingDefinition mapping) {
		super.copyTo(mapping);
		
		mapping.setInputField(inputField);
		mapping.setOutputField(outputField);
	}

	@Override
	public XMLMappingDefinition createCopy() {
		InputFieldMappingDefinition mapping = new InputFieldMappingDefinition();
		this.copyTo(mapping);
		
		return mapping;
	}	
	
	public String getInputField() {
		return inputField;
	}

	public void setInputField(String inputField) {
		this.inputField = inputField;
	}

	public String getOutputField() {
		return outputField;
	}

	public void setOutputField(String outputField) {
		this.outputField = outputField;
	}
}

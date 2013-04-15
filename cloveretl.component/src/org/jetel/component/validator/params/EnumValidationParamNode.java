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
package org.jetel.component.validator.params;

/**
 * Parameter of validation rule which can value from given enum value and can be (de)serialized.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.11.2012
 */
final public class EnumValidationParamNode extends ValidationParamNode {
	Enum value;
	Enum[] options;
	
	@SuppressWarnings("unused")
	private EnumValidationParamNode() {} // For JAXB
	
	public EnumValidationParamNode(Enum[] options, Enum value) {
		this.options = options;
		setValue(value);
		
	}	
	public void setValue(Enum value) {
		for(Object option : options) {
			if(option.equals(value)) {
				this.value = value;
				return;
			}
		}
	}
	public Enum getValue() {
		return value;
	}

	/**
	 * Sets enum value from its string representation (due to JAXB issues)
	 * @param input
	 */
	public void setFromString(String input) {
		for(Enum option : options) {
			if(option.name().equals(input)) {
				value = option;
				return;
			}
		}
	}
	public Enum[] getOptions() {
		return options;
	}
	
	@Override
	public String toString() {
		return value.name();
	}

}

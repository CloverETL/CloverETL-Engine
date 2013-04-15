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

import javax.xml.bind.annotation.XmlAttribute;

/**
 * Parameter of validation rule which can contain integer and can be (de)serialized.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 28.11.2012
 */
final public class IntegerValidationParamNode extends ValidationParamNode {
	@XmlAttribute
	Integer value;
	
	public IntegerValidationParamNode() {}
	
	public IntegerValidationParamNode(Integer value) {
		this.value = value;
	}
	
	public Integer getValue() {
		return value;
	}
	public void setValue(Integer other) {
		value = other;
	}
	
	@Override
	public String toString() {
		if(value == null) {
			return "null"; // Due to usage for logging (no null alowed)
		}
		return value.toString();
	}

}

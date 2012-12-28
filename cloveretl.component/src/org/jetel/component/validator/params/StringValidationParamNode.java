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

import javax.xml.bind.annotation.XmlValue;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 28.11.2012
 */
final public class StringValidationParamNode extends ValidationParamNode {
	@XmlValue
	String value;
	
	@SuppressWarnings("unused")
	private StringValidationParamNode(){} // for JAXB
	
	public StringValidationParamNode(String key, String name) {
		super(key, name);
	}
	
	public StringValidationParamNode(String key, String name, String value) {
		super(key, name);
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}
	public void setValue(String other) {
		value = other;
	}

}

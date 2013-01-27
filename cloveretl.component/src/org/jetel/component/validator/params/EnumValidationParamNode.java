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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlValue;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.11.2012
 */
final public class EnumValidationParamNode extends ValidationParamNode {
	@XmlValue
	String value = "";
	Object realValue;
	LinkedHashMap<Object, String> options;
	
	@SuppressWarnings("unused")
	private EnumValidationParamNode() {} // For JAXB
	
	public EnumValidationParamNode(int key, String name, Map<Object, String> options, Object value) {
		super(key, name);
		this.options = new LinkedHashMap<Object, String>(options);
		setValue(value);
	}
	
	public Object getValue() {
		return realValue;
	}
	public void setValue(Object other) {
		if(!options.containsKey(other)) return;
		realValue = other;
		value = other.toString();
	}
	public Map<Object, String> getOptions() {
		return options;
	}

}

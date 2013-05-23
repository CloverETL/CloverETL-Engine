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
package org.jetel.component.validator.utils;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

import org.jetel.component.validator.CustomRule;
import org.jetel.component.validator.ValidationGroup;

/**
 * Wrapper class for serializing map of custom rules to/from XML.
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.4.2013
 * @see ValidationGroup
 * @see CustomRulesMapAdapter
 * @see CustomRule
 */
public class AdaptedCustomRule {
	@XmlAttribute(name="id")
	private int id;
	
	@XmlAttribute(name="name")
	private String name;
	
	@XmlValue
	private String code;
	
	private AdaptedCustomRule() {} // For JAXB
	
	public AdaptedCustomRule(int id, String name, String code) {
		this.id = id;
		this.name = name;
		this.code = code;
	}
	
	public String getCode() {
		return code;
	}
	
	public String getName() {
		return name;
	}
	
	public int getId() {
		return id;
	}
}

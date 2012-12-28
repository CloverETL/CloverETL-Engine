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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlSeeAlso;

import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.rules.EnumMatchValidationRule;
import org.jetel.component.validator.rules.NonEmptyFieldValidationRule;
import org.jetel.component.validator.rules.NonEmptySubsetValidationRule;
import org.jetel.component.validator.rules.PatternMatchValidationRule;
import org.jetel.component.validator.rules.StringLengthValidationRule;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
public abstract class AbstractValidationRule extends ValidationNode {
	
	public final static String TARGET = "target"; 
	
	Map<String,ValidationParamNode> params = new HashMap<String, ValidationParamNode>();
	
	public ValidationParamNode getParam(String key) {
		return params.get(key);
	}
	
	protected void addParamNode(ValidationParamNode pn) {
		params.put(pn.getKey(), pn);
	}
	public List<ValidationParamNode> getParamNodes() {
		return new ArrayList<ValidationParamNode>(params.values());
	}
}

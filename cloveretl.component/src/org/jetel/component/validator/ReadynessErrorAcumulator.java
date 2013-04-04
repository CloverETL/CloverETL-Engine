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

import org.jetel.component.validator.params.ValidationParamNode;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 1.3.2013
 */
public class ReadynessErrorAcumulator {
	private Map<ValidationParamNode, List<String>> errors = new HashMap<ValidationParamNode, List<String>>();
	private Map<ValidationParamNode, ValidationNode> parents = new HashMap<ValidationParamNode, ValidationNode>();
	
	public void addError(ValidationParamNode node, ValidationNode parent, String message) {
		if(!hasErrors(node)) {
			errors.put(node, new ArrayList<String>());
		}
		if(!parents.containsKey(node)) {
			parents.put(node, parent);
		}
		errors.get(node).add(message);
	}
	public boolean hasErrors(ValidationParamNode node) {
		return errors.containsKey(node);
	}
	public List<String> getErrors(ValidationParamNode node) {
		return errors.get(node);
	}
	public Map<ValidationParamNode, List<String>> getErrors() {
		return errors;
	}
	public ValidationNode getParentRule(ValidationParamNode node) {
		return parents.get(node);
	}
	
	public String getAllErrorsInString() {
		StringBuilder output = new StringBuilder();
		for(List<String> temp : errors.values()) {
			for(String temp2 : temp) {
				output.append(temp2);
				output.append("\n");
			}
		}
		return output.toString().trim();
	}
	
	public void reset() {
		errors.clear();
		parents.clear();
	}
}

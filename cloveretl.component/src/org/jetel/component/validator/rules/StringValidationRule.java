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

import javax.xml.bind.annotation.XmlElement;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.data.DataField;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 4.12.2012
 */
public abstract class StringValidationRule extends AbstractValidationRule{
	public final static int TRIM = 10;
	
	@XmlElement(name="trimInput",required=true)
	protected BooleanValidationParamNode trimInput = new BooleanValidationParamNode(TRIM, "", false);

	public StringValidationRule() {
		this.addParamNode(trimInput);
	}
	
	protected String prepareInput(DataField df) {
		String out = df.toString();
		if(trimInput.getValue()) {
			return out.trim();
		}
		return out;
	}
}

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
import org.jetel.component.validator.ValidatorMessages;
import org.jetel.component.validator.params.MappingValidationParamNode;

/**
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.6.2013
 */
public abstract class AbstractMappingValidationRule extends AbstractValidationRule {

	@XmlElement(name="mapping",required=true)
	protected MappingValidationParamNode mappingParam = new MappingValidationParamNode();
	
	@Override
	final public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.MAPPING_SPECIFIED;
	}
	
	public abstract String[] getMappingTargetFields();
	
	public MappingValidationParamNode getMappingParam() {
		return mappingParam;
	}
	
	public abstract String getDetailName();
	public abstract String getMappingName();
	public abstract String getTargetMappedItemName();
	
	public String getSourceMappedItemName() {
		return ValidatorMessages.getString("AbstractMappingValidationRule.SourceMappedItemName"); //$NON-NLS-1$
	}

}

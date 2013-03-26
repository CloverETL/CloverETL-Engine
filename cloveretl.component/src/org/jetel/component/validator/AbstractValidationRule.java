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

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.rules.ComparisonValidationRule;
import org.jetel.component.validator.rules.DateValidationRule;
import org.jetel.component.validator.rules.EnumMatchValidationRule;
import org.jetel.component.validator.rules.IntervalValidationRule;
import org.jetel.component.validator.rules.LookupValidationRule;
import org.jetel.component.validator.rules.NonEmptyFieldValidationRule;
import org.jetel.component.validator.rules.NonEmptySubsetValidationRule;
import org.jetel.component.validator.rules.NumberValidationRule;
import org.jetel.component.validator.rules.PatternMatchValidationRule;
import org.jetel.component.validator.rules.StringLengthValidationRule;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.11.2012
 */
@XmlSeeAlso({
		ValidationGroup.class,
		EnumMatchValidationRule.class,
		NonEmptyFieldValidationRule.class, 
		NonEmptySubsetValidationRule.class, 
		PatternMatchValidationRule.class, 
		StringLengthValidationRule.class,
		IntervalValidationRule.class,
		ComparisonValidationRule.class,
		DateValidationRule.class,
		NumberValidationRule.class,
		LookupValidationRule.class
	})
public abstract class AbstractValidationRule extends ValidationNode {
	
	public static enum TARGET_TYPE {
		ONE_FIELD, UNORDERED_FIELDS, ORDERED_FIELDS
	}
	
	@XmlElement(name="target",required=true)
	protected StringValidationParamNode target = new StringValidationParamNode();
	
	private List<ValidationParamNode> params;
	
	public List<ValidationParamNode> getParamNodes(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		if(params == null) {
			params = initialize(inMetadata, graphWrapper);
		}
		return params;
	}
	
	protected abstract List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper);
	
	public StringValidationParamNode getTarget() {
		return target;
	}
	
	public abstract TARGET_TYPE getTargetType();
}

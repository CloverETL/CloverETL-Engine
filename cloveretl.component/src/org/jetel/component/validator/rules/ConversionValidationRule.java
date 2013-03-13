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

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.EnumValidationParamNode;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode.EnabledHandler;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.data.DataField;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 12.3.2013
 */
@XmlType(propOrder={"useTypeJAXB", "format", "strict" })
public abstract class ConversionValidationRule extends AbstractValidationRule {

	public static enum METADATA_TYPES {
		DEFAULT, STRING, DATE, LONG, NUMBER, DECIMAL;
		@Override
		public String toString() {
			if(this.equals(DEFAULT)) {
				return "type from metadata";
			}
			if(this.equals(STRING)) {
				return "strings";
			}
			if(this.equals(DATE)) {
				return "dates";
			}
			if(this.equals(LONG)) {
				return "longs";
			}
			if(this.equals(NUMBER)) {
				return "numbers";
			}
			return "decimals";
		}
	}
	
	protected EnumValidationParamNode useType = new EnumValidationParamNode(METADATA_TYPES.values(), METADATA_TYPES.DEFAULT);
	@XmlElement(name="useType")
	private String getUseTypeJAXB() { return ((Enum<?>) useType.getValue()).name(); }
	private void setUseTypeJAXB(String input) { this.useType.setFromString(input); }
	
	@XmlElement(name="format", required=false)
	protected StringEnumValidationParamNode format = new StringEnumValidationParamNode();
	
	@XmlElement(name="strict", required=true)
	protected BooleanValidationParamNode strict = new BooleanValidationParamNode(false);
	
	@Override
	protected List<ValidationParamNode> initialize() {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		useType.setName("Compare as");
		params.add(useType);
		
		format.setName("Format mask");
		format.setOptions(CommonFormats.all);
		format.setPlaceholder("Number/date format, for syntax see documentation.");
		format.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				if(useType.getValue() == METADATA_TYPES.DATE || useType.getValue() == METADATA_TYPES.NUMBER || useType.getValue() == METADATA_TYPES.DECIMAL) {
					return true;
				}
				return false;
			}
		});
		params.add(format);
		strict.setName("Strict mode");
		strict.setEnabledHandler(new EnabledHandler() {
			
			@Override
			public boolean isEnabled() {
				if(useType.getValue() == METADATA_TYPES.DATE || useType.getValue() == METADATA_TYPES.NUMBER || useType.getValue() == METADATA_TYPES.DECIMAL) {
					return true;
				}
				return false;
			}
		});
		params.add(strict);	
		return params;
	}
	
	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator) {
		if(useType.getValue() == METADATA_TYPES.DATE || useType.getValue() == METADATA_TYPES.NUMBER || useType.getValue() == METADATA_TYPES.DECIMAL) {
			if(format.getValue().isEmpty() && inputMetadata.getField(target.getValue()).getDataType() == DataFieldType.STRING) {
				accumulator.addError(format, this, "Format mask to parse incoming field, value from and to must be filled.");
				return false;
			}
		}
		return true;
	}
	
	protected DataFieldType computeType(DataField field) {
		DataFieldType fieldType = field.getMetadata().getDataType();
		if(useType.getValue() == METADATA_TYPES.STRING) {
			fieldType = DataFieldType.STRING;
		} else if(useType.getValue() == METADATA_TYPES.DATE) {
			fieldType = DataFieldType.DATE;
		} else if(useType.getValue() == METADATA_TYPES.LONG) {
			fieldType = DataFieldType.LONG;
		} else if(useType.getValue() == METADATA_TYPES.NUMBER) {
			fieldType = DataFieldType.NUMBER;
		} else if(useType.getValue() == METADATA_TYPES.DECIMAL) {
			fieldType = DataFieldType.DECIMAL;
		}
		return fieldType;
	}

	public BooleanValidationParamNode getStrict() {
		return strict;
	}
	
	public StringEnumValidationParamNode getFormat() {
		return format;
	}

	public EnumValidationParamNode getUseType() {
		return useType;
	}
}

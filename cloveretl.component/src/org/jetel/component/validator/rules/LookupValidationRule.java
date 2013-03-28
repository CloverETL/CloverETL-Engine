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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.params.StringEnumValidationParamNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidatorUtils;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 25.3.2013
 */
@XmlRootElement(name="lookup")
@XmlType(propOrder={"lookup"})
public class LookupValidationRule extends AbstractValidationRule {
	@XmlElement(name="lookup",required=true)
	private StringEnumValidationParamNode lookup = new StringEnumValidationParamNode();

	@Override
	protected List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
		ArrayList<ValidationParamNode> params = new ArrayList<ValidationParamNode>();
		lookup.setName("Lookup name");
		HashSet<String> lookups = new HashSet<String>();
		Iterator<String> temp = graphWrapper.getLookupTables();
		while(temp.hasNext()) {
			lookups.add(temp.next());
		}
		lookup.setOptions(lookups.toArray(new String[0]));
		params.add(lookup);
		return params;
	}

	@Override
	public TARGET_TYPE getTargetType() {
		return TARGET_TYPE.ONE_FIELD;
	}

	@Override
	public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(!isEnabled()) {
			logger.trace("Validation rule: " + getName() + " is " + State.NOT_VALIDATED);
			return State.NOT_VALIDATED;
		}
		LookupTable lookupTable = graphWrapper.getLookupTable(lookup.getValue());
		if(lookupTable == null) {
			logger.trace("Validation rule: " + getName() + " is " + State.INVALID + " (unknown lookup table)");
			return State.INVALID;
		}
		DataRecordMetadata metadata = null;
		try {
			metadata = lookupTable.getKeyMetadata();
		} catch (Exception ex) {
			logger.trace("Validation rule: " + getName() + " is " + State.INVALID + " (no key metadata)");
			return State.INVALID;
		}
		Lookup lookup = null;
		try {
			lookup = lookupTable.createLookup(new RecordKey(metadata.getFieldNamesArray(), metadata));
		} catch (Exception ex) {
			logger.trace("Validation rule: " + getName() + " is " + State.INVALID + " (lookup not ready)");
			return State.INVALID;
		}
		System.err.println(lookup);
		System.err.println(lookup.getKey());
		while (lookup.hasNext()) {
			System.err.println(lookup.next());
		}
		lookupTable.free();
		return State.INVALID;
	}

	@Override
	public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator) {
		if(!isEnabled()) {
			return true;
		}
		boolean state = true;
		if(target.getValue().isEmpty()) {
			accumulator.addError(target, this, "Target is empty.");
			state = false;
		}
		if(!ValidatorUtils.isValidField(target.getValue(), inputMetadata)) { 
			accumulator.addError(target, this, "Target field is not present in input metadata.");
			state = false;
		}
		if(lookup.getValue().isEmpty()) {
			accumulator.addError(lookup, this, "No lookup table provided.");
			state = false;
		}
		return state;
	}

	@Override
	public String getCommonName() {
		return "Lookup";
	}

	@Override
	public String getCommonDescription() {
		return "Checks if there is a record in lookup table which match value of chosen field."; 
	}
	

}

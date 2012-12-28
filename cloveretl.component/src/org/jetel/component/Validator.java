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
package org.jetel.component;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.params.BooleanValidationParamNode;
import org.jetel.component.validator.params.IntegerValidationParamNode;
import org.jetel.component.validator.params.StringSetValidationParamNode;
import org.jetel.component.validator.params.StringValidationParamNode;
import org.jetel.component.validator.rules.EnumMatchValidationRule;
import org.jetel.component.validator.rules.NonEmptyFieldValidationRule;
import org.jetel.component.validator.rules.NonEmptySubsetValidationRule;
import org.jetel.component.validator.rules.PatternMatchValidationRule;
import org.jetel.component.validator.rules.StringLengthValidationRule;
import org.jetel.component.validator.utils.ValidationRulesPersister;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 23.10.2012
 */
public class Validator extends Node {
	
	private final static Log logger = LogFactory.getLog(Validator.class);
	private static final String COMPONENT_TYPE = "VALIDATOR";
	
	public static final String XML_VALIDATION_RULES = "rules";
	
	private final static int INPUT_PORT = 0;
	private final static int VALID_OUTPUT_PORT = 0;
	private final static int INVALID_OUTPUT_PORT = 1;
	
	private int processedRecords = 0;
	
	private ValidationGroup rootGroup;
	
	
	/**
	 * Minimalistic constructor to ensure component name init
	 * @param id
	 */
	public Validator(String id) {
		super(id);
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}
	
	public static Node fromXML(TransformationGraph graph, Element xmlElement)throws XMLConfigurationException {
		Validator validator;
	
		ComponentXMLAttributes attrs = new ComponentXMLAttributes(xmlElement, graph);
		try {
			validator = new Validator(attrs.getString(XML_ID_ATTRIBUTE));
			if(attrs.exists(XML_VALIDATION_RULES)) {
				System.out.println(attrs.getString(XML_VALIDATION_RULES));
				validator.setRootGroup(ValidationRulesPersister.deserialize(attrs.getString(XML_VALIDATION_RULES)));
			}
			return validator;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ": Invalid XML configuration.", ex);
		}
	}
	
	public ValidationGroup getRootGroup() {
		return rootGroup;
	}
	public void setRootGroup(ValidationGroup group) {
		rootGroup = group;
	}

	@Override
	protected Result execute() throws Exception {
		logger.trace("Executing Validator component");
		// Prepare validation tree
		ValidationGroup root = rootGroup;
		
		// Prepare ports and structures for records
		InputPortDirect inPort = getInputPortDirect(INPUT_PORT);
		OutputPortDirect validPort = getOutputPortDirect(VALID_OUTPUT_PORT);
		OutputPortDirect invalidPort = getOutputPortDirect(INVALID_OUTPUT_PORT);
		
		DataRecord record = DataRecordFactory.newRecord(getInputPort(INPUT_PORT).getMetadata());
		CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		record.init();
		
		
		// Iterate over data
		boolean hasData = true;
		while(hasData && runIt) {
			ValidationErrorAccumulator ea = new ValidationErrorAccumulator();
			if(!inPort.readRecordDirect(recordBuffer)) {
				hasData = false;
				continue;
			}
			processedRecords++;
			logger.trace("Validation of record number " + processedRecords + " has started.");
			record.deserialize(recordBuffer);
			if(root.isValid(record,ea) != ValidationNode.State.INVALID) {
				recordBuffer.rewind();
				validPort.writeRecordDirect(recordBuffer);
				logger.trace("Record number " + processedRecords + " is VALID.");
			} else {
				recordBuffer.rewind();
				if(invalidPort != null) {
					invalidPort.writeRecordDirect(recordBuffer);
				}
				logger.trace("Record number " + processedRecords + " is INVALID.");
			}
			logger.trace("Validation of record number " + processedRecords + " has finished.");
		}
		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

}

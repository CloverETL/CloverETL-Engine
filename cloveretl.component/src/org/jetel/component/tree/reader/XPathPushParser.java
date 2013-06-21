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
package org.jetel.component.tree.reader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jetel.collection.Stack;
import org.jetel.component.tree.reader.mappping.FieldMapping;
import org.jetel.component.tree.reader.mappping.MappingContext;
import org.jetel.component.tree.reader.mappping.MappingElement;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelRuntimeException;

/**
 * This unit traverses tree structure using given mapping. Results of the traversal are pushed into
 * {@link DataRecordReceiver}.
 * 
 * @author jan.michalica (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 5.12.2011
 */
public class XPathPushParser {

	protected DataRecordProvider recordProvider;
	protected DataRecordReceiver recordReceiver;
	protected XPathEvaluator evaluator;
	protected ValueHandler valueHandler;
	protected XPathSequenceProvider sequenceProvider;
	
	protected Stack<Boolean> contextErrorStack = new Stack<Boolean>();

	/**
	 * Constructs new XPath push parser.
	 * 
	 * @param provider
	 *            - to obtain data record for given output port
	 * @param receiver
	 *            - to notify data record has been filled
	 * @param evaluator
	 *            - to evaluate XPath over given data context
	 * @param sequenceProvider
	 *            - to fetch sequences defined in mapping
	 */
	public XPathPushParser(DataRecordProvider provider, DataRecordReceiver receiver, XPathEvaluator evaluator,
			ValueHandler valueHandler, XPathSequenceProvider sequenceProvider) {

		this.recordProvider = provider;
		this.recordReceiver = receiver;
		this.evaluator = evaluator;
		this.valueHandler = valueHandler;
		this.sequenceProvider = sequenceProvider;
	}

	/**
	 * Process given input into data records using provided mapping.
	 * 
	 * @param mapping
	 * @param input
	 */
	public void parse(MappingContext mapping, Object input) throws AbortParsingException {

		handleContext(mapping, input, null);
		evaluator.reset();
	}

	protected void handleContext(MappingContext mapping, Object context, DataRecord dataTarget)
			throws AbortParsingException {
		/*
		 * for unbound context, switch evaluation context and process nested mappings
		 */
		if (mapping.getOutputPort() == null) {
			Object newContext = evaluator.evaluatePath(mapping.getXPath(), getNamespaceBinding(mapping), context, mapping);
			applyContextMappings(mapping, newContext, dataTarget, -1);
			return;
		}
		
		contextErrorStack.push(Boolean.FALSE);

		/*
		 * iterate through XPath results
		 */
		Iterator<Object> it = evaluator.iterate(mapping.getXPath(), getNamespaceBinding(mapping), context, mapping);
		/*
		 * prepare keys to be bound from children (if any)
		 */
		DataField parentKeyFields[] = null;
		String parentKeys[] = mapping.getParentKeys();
		if (dataTarget != null && parentKeys != null) {
			parentKeyFields = new DataField[parentKeys.length];
			for (int i = 0; i < parentKeys.length; ++i) {
				parentKeyFields[i] = dataTarget.getField(parentKeys[i]);
			}
		}
		
		int portIndex = mapping.getOutputPort().intValue();
		Sequence sequence = mapping.getSequenceField() == null ? null : getSequence(mapping);
		while (it.hasNext()) {
			DataRecord newTarget = recordProvider.getDataRecord(portIndex);
			/*
			 * process sequence (if any)
			 */
			if (mapping.getSequenceField() != null) {
				fillSequenceField(mapping.getSequenceField(), newTarget, sequence, portIndex);
			}
			/*
			 * process keying from child to parent (if any)
			 */
			String generatedKeys[] = mapping.getGeneratedKeys();
			if (parentKeys != null && generatedKeys != null) {
				for (int i = 0; i < generatedKeys.length; ++i) {
					DataField currentRecordParentKeyField = newTarget.getField(generatedKeys[i]);
					try {
						currentRecordParentKeyField.setValue(parentKeyFields[i]);
					} catch (BadDataFormatException e) {
						handleException(portIndex, newTarget, currentRecordParentKeyField, e);
					}
				}
			}
			/*
			 * process nested mappings on each element
			 */
			Object element = it.next();
			applyContextMappings(mapping, element, newTarget, portIndex);

			/*
			 * pass record data to consumer if no error occured
			 */
			if (contextErrorStack.peek() == Boolean.FALSE) {
				recordReceiver.receive(newTarget, portIndex);
			} else {
				// replace error state with OK state
				contextErrorStack.pop(); 
				contextErrorStack.push(Boolean.FALSE);
			}
		}
		contextErrorStack.pop();
	}

	protected void applyContextMappings(MappingContext mapping, Object evaluationContext, DataRecord targetRecord, int portIndex)
			throws AbortParsingException {

		if (evaluationContext == null) {
			return;
		}
		for (FieldMapping fieldMapping : mapping.getFieldMappingChildren()) {
			handleFieldMapping(fieldMapping, evaluationContext, targetRecord, portIndex);
		}
		for (MappingContext constantMapping : mapping.getMappingContextChildren()) {
			handleContext(constantMapping, evaluationContext, targetRecord);
		}
	}

	protected void handleFieldMapping(FieldMapping mapping, Object context, DataRecord target, int portIndex)
			throws AbortParsingException {

		DataField field = null;
		if (target == null) {
			throw new JetelRuntimeException("Cannot perform field mapping for '" + mapping.getCloverField() + "' as there was no output port specified");
		}
		try {
			field = target.getField(mapping.getCloverField());
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new RuntimeException("field " + mapping.getCloverField() + " not found on " + target);
		}
		Object value = null;
		if (mapping.getXPath() != null) {
			value = evaluator.evaluatePath(mapping.getXPath(), getNamespaceBinding(mapping), context, mapping);
		} else {
			value = evaluator.evaluateNodeName(mapping.getNodeName(), getNamespaceBinding(mapping), context, mapping);
		}
		if (value != null) {
			try {
				valueHandler.storeValueToField(value, field, mapping.isTrim());
			} catch (BadDataFormatException e) {
				handleException(portIndex, target, field, e);
			}
		}
	}

	protected void fillSequenceField(String fieldName, DataRecord record, Sequence sequence, int portIndex)
			throws AbortParsingException {

		DataField field = null;
		try {
			field = record.getField(fieldName);
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new RuntimeException("field " + fieldName + " not found on " + record);
		}
		try {
			switch (field.getMetadata().getDataType()) {
			case INTEGER: {
				field.setValue(Integer.valueOf(sequence.nextValueInt()));
				break;
			}
			case DECIMAL:
			case NUMBER:
			case LONG: {
				field.setValue(Long.valueOf(sequence.nextValueLong()));
				break;
			}
			case STRING: {
				field.setValue(sequence.nextValueString());
				break;
			}
			}
		} catch (BadDataFormatException e) {
			handleException(portIndex, record, field, e);
		}
	}

	protected Sequence getSequence(MappingContext context) {
		return sequenceProvider.getSequence(context);
	}

	private void handleException(int portIndex, DataRecord newTarget, DataField currentRecordParentKeyField, BadDataFormatException e) throws AbortParsingException {
		recordReceiver.exceptionOccurred(generateException(e, currentRecordParentKeyField, newTarget, portIndex));
		contextErrorStack.pop();
		contextErrorStack.push(Boolean.TRUE);
	}

	private FieldFillingException generateException(BadDataFormatException ex, DataField field, DataRecord record, int portIndex) {
		FieldFillingException newEx = new FieldFillingException(ex);
		newEx.setFieldMetadata(field.getMetadata());
		newEx.setIncompleteRecord(record);
		newEx.setPortIndex(portIndex);
		return newEx;
	}
	
	private Map<String, String> getNamespaceBinding(MappingElement mappingElement) {
		
		Map<String, String> bindings = null;
		while (mappingElement != null) {
			Map<String, String> binding = mappingElement.getNamespaceBinding();
			if (!binding.isEmpty()) {
				if (bindings == null) {
					bindings = new HashMap<String, String>();
				}
				bindings.putAll(binding);
			}
			mappingElement = mappingElement.getParent();
		}
		return bindings == null ? Collections.<String, String>emptyMap() : bindings;
	}
}

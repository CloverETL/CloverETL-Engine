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

import java.util.Iterator;

import org.jetel.component.tree.reader.mappping.FieldMapping;
import org.jetel.component.tree.reader.mappping.MappingContext;
import org.jetel.component.tree.reader.mappping.MappingElement;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.BadDataFormatException;

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
			Object newContext = evaluator.evaluatePath(mapping.getXPath(), mapping.getNamespaceBinding(), context, mapping);
			applyContextMappings(mapping, newContext, dataTarget);
			return;
		}
		/*
		 * iterate through XPath results
		 */
		Iterator<Object> it = evaluator.iterate(mapping.getXPath(), mapping.getNamespaceBinding(), context, mapping);
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
		while (it.hasNext()) {
			DataRecord newTarget = recordProvider.getDataRecord(portIndex);
			/*
			 * process sequence (if any)
			 */
			if (mapping.getSequenceField() != null) {
				fillSequenceField(mapping.getSequenceField(), newTarget, getSequence(mapping));
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
						recordReceiver.exceptionOccurred(e);
					}
				}
			}
			/*
			 * process nested mappings on each element
			 */
			Object element = it.next();
			applyContextMappings(mapping, element, newTarget);
			/*
			 * pass record data to consumer
			 */
			recordReceiver.receive(newTarget, portIndex);
		}
	}

	protected void applyContextMappings(MappingContext mapping, Object evaluationContext, DataRecord targetRecord)
			throws AbortParsingException {

		if (evaluationContext == null) {
			return;
		}
		for (MappingElement element : mapping.getChildren()) {
			if (element instanceof FieldMapping) {
				FieldMapping fieldMapping = (FieldMapping) element;
				handleFieldMapping(fieldMapping, evaluationContext, targetRecord);
			} else if (element instanceof MappingContext) {
				MappingContext nestedContext = (MappingContext) element;
				handleContext(nestedContext, evaluationContext, targetRecord);
			}
		}
	}

	protected void handleFieldMapping(FieldMapping mapping, Object context, DataRecord target)
			throws AbortParsingException {

		DataField field = null;
		try {
			field = target.getField(mapping.getCloverField());
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new RuntimeException("field " + mapping.getCloverField() + " not found on " + target);
		}
		Object value = null;
		if (mapping.getXPath() != null) {
			value = evaluator.evaluatePath(mapping.getXPath(), mapping.getNamespaceBinding(), context, mapping);
		} else {
			value = evaluator.evaluateNodeName(mapping.getNodeName(), mapping.getNamespaceBinding(), context, mapping);
		}
		if (value != null) {
			// TODO: reimplement trim functionality
			try {
				valueHandler.storeValueToField(value, field);
			} catch (BadDataFormatException e) {
				recordReceiver.exceptionOccurred(e);
			}
			// if (field.getType() == DataFieldMetadata.STRING_FIELD
			// && value instanceof String
			// && mapping.isTrim()) {
			// value = ((String)value).trim();
			//
			// }
			// try {
			// field.setValue(value);
			// } catch (BadDataFormatException e) {
			// recordReceiver.exceptionOccurred(e);
			// }
		}
	}

	protected void fillSequenceField(String fieldName, DataRecord record, Sequence sequence)
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
			recordReceiver.exceptionOccurred(e);
		}
	}

	protected Sequence getSequence(MappingContext context) {
		return sequenceProvider.getSequence(context);
	}
}

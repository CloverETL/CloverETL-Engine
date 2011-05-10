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
package org.jetel.component.xml.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.jetel.component.xml.writer.MappingTagger.Tag;
import org.jetel.component.xml.writer.mapping.AbstractElement;
import org.jetel.component.xml.writer.mapping.Attribute;
import org.jetel.component.xml.writer.mapping.Comment;
import org.jetel.component.xml.writer.mapping.Element;
import org.jetel.component.xml.writer.mapping.MappingProperty;
import org.jetel.component.xml.writer.mapping.Namespace;
import org.jetel.component.xml.writer.mapping.Relation;
import org.jetel.component.xml.writer.mapping.Value;
import org.jetel.component.xml.writer.mapping.WildcardElement;
import org.jetel.component.xml.writer.mapping.XmlMapping;
import org.jetel.component.xml.writer.model.DynamicValue;
import org.jetel.component.xml.writer.model.StaticValue;
import org.jetel.component.xml.writer.model.TextValue;
import org.jetel.component.xml.writer.model.WritableAttribute;
import org.jetel.component.xml.writer.model.WritableComment;
import org.jetel.component.xml.writer.model.WritableElement;
import org.jetel.component.xml.writer.model.WritableLoopElement;
import org.jetel.component.xml.writer.model.WritableMapping;
import org.jetel.component.xml.writer.model.WritableNamespace;
import org.jetel.component.xml.writer.model.WritableValue;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Mapping compiler, which processes xml mapping and generates engine model. Assumes that xml mapping is valid!
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15 Dec 2010
 */
public class MappingCompiler extends AbstractVisitor {
	
	private final Map<Integer, DataRecordMetadata> inPorts;
	private TransformationGraph graph;
	private String componentId;
	private Log logger;
	
	private Map<Integer, PortData> portDataMap;
	private Map<Element, Tag> tagMap;
	
	private Map<Element, WritableElement> compiledMap = new HashMap<Element, WritableElement>();
	private Stack<Integer> availableData = new Stack<Integer>();
	private Set<Element> addedPorts = new HashSet<Element>();
	
	private Element modelPartitionElement;
	private WritableLoopElement partitionElement;
	
	private WritableElement root;
	private WritableElement currentParent;
	private WritableLoopElement currentLoopParent;
	
	private MappingTagger tagger;
	
	public MappingCompiler(Map<Integer, DataRecordMetadata> inPorts, String sortHintsString) {
		this.inPorts = inPorts;
		this.tagger = new MappingTagger(inPorts, sortHintsString);
	}

	public WritableMapping compile(Map<Integer, InputPort> inPorts, String tmpDir, long cacheSize, boolean partition) throws ComponentNotReadyException {
		tagger.setResolvePartition(partition);
		tagger.tag();
		
		tagMap = tagger.getTagMap();
		portDataMap = tagger.getPortDataMap(inPorts, tmpDir, cacheSize);
		modelPartitionElement = tagger.getPartitionElement();
		
		mapping.visit(this);
		return new WritableMapping(mapping.getVersion(), root, partitionElement);
	}
	
	public static int resolvePartitionKeyPortIndex(XmlMapping mapping, Map<Integer, DataRecordMetadata> inPorts) {
		MappingTagger tagger = new MappingTagger(inPorts, null);
		tagger.setResolvePartition(true);
		tagger.setMapping(mapping);
		tagger.tag();
		return tagger.getPartitionElementPortIndex();
	}
	
	public void setMapping(XmlMapping mapping) {
		super.setMapping(mapping);
		tagger.setMapping(mapping);
	}
	
	public Map<Integer, PortData> getPortDataMap() {
		return portDataMap;
	}
	
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}

	public void setLogger(Log logger) {
		this.logger = logger;
	}

	@Override
	public void visit(WildcardElement element) throws Exception {
		String include = element.getProperty(MappingProperty.INCLUDE);
		String exclude = element.getProperty(MappingProperty.EXCLUDE);
		
		String writeNull;
		String omitNull;
		if (element.isElement()) {
			writeNull = element.getProperty(MappingProperty.WRITE_NULL_ELEMENT);
			omitNull = element.getProperty(MappingProperty.OMIT_NULL_ELEMENT);
		} else {
			writeNull = element.getParent().getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
			omitNull = element.getParent().getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);
		}
		
		List<String> attributeNames = new ArrayList<String>();
		for (Attribute attribute : element.getParent().getAttributes()) {
			attributeNames.add(attribute.getProperty(MappingProperty.NAME));
		}
		
		List<DataFieldMetadataWrapper> availableFields = new LinkedList<DataFieldMetadataWrapper>();
		if (include != null) {
			for (String aggregateExpression : include.split(XmlMapping.DELIMITER)) {
				if (aggregateExpression.matches(MappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN)) {
					availableFields.addAll(getFields(aggregateExpression, element.getParent()));
				}
			}
		} else {
			for (Integer inputPortIndex : availableData) {
				DataRecordMetadata metadata = inPorts.get(inputPortIndex);
				DataFieldMetadata[] fields = metadata.getFields();
				for (int i = 0; i < fields.length; i++) {
					availableFields.add(new DataFieldMetadataWrapper(inputPortIndex, i, fields[i], null));
				}
			}
		}
		if (exclude != null) {
			for (String aggregateExpression : exclude.split(XmlMapping.DELIMITER)) {
				if (aggregateExpression.matches(MappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN)) {
					availableFields.removeAll(getFields(aggregateExpression, element.getParent()));
				}
			}
		}
		Set<DataFieldMetadataWrapper> writeNullSet = new HashSet<DataFieldMetadataWrapper>();
		if (writeNull != null) {
			for (String aggregateExpression : writeNull.split(XmlMapping.DELIMITER)) {
				if (aggregateExpression.matches(MappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN)) {
					writeNullSet.addAll(getFields(aggregateExpression, element.getParent()));
				}
			}
		} else if (omitNull != null) {
			for (Integer inputPortIndex : availableData) {
				DataRecordMetadata metadata = inPorts.get(inputPortIndex);
				DataFieldMetadata[] fields = metadata.getFields();
				for (int i = 0; i < fields.length; i++) {
					writeNullSet.add(new DataFieldMetadataWrapper(inputPortIndex, i, fields[i], null));
				}
			}
		}
		if (omitNull != null) {
			for (String aggregateExpression : omitNull.split(XmlMapping.DELIMITER)) {
				if (aggregateExpression.matches(MappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN)) {
					writeNullSet.removeAll(getFields(aggregateExpression, element.getParent()));
				}
			}
		}
		
		if (element.isElement()) {
			for (DataFieldMetadataWrapper dataFieldWrapper : availableFields) {
				WritableValue value = WritableValue.newInstance(new TextValue[] {new DynamicValue(dataFieldWrapper.port, dataFieldWrapper.fieldIndex)});
				WritableElement subElement = new WritableElement(dataFieldWrapper.dataFieldMetadata.getName(),
						dataFieldWrapper.namespace, writeNullSet.contains(dataFieldWrapper));
				subElement.addChild(value);
				currentParent.addChild(subElement);
			}
		} else {
			for (DataFieldMetadataWrapper dataFieldWrapper : availableFields) {
				WritableValue value = WritableValue.newInstance(new TextValue[] {new DynamicValue(dataFieldWrapper.port, dataFieldWrapper.fieldIndex)});				
				WritableAttribute attribute = new WritableAttribute(dataFieldWrapper.dataFieldMetadata.getName(),
						dataFieldWrapper.namespace, value, writeNullSet.contains(dataFieldWrapper));
				currentParent.addAttribute(attribute);
			}
		}
	}

	private Set<DataFieldMetadataWrapper> getFields(String aggregateExpression, Element parent) {
		ParsedFieldExpression parsed = parseAggregateExpression(aggregateExpression);
		Integer inputPortIndex = getFirstLocalPortIndex(parsed.getPort(), availableData, inPorts);
		DataRecordMetadata metadata = inPorts.get(inputPortIndex);

		Set<DataFieldMetadataWrapper> availableFields = new LinkedHashSet<DataFieldMetadataWrapper>();
		for (int i = 0; i < metadata.getNumFields(); i++) {
			DataFieldMetadata field = metadata.getField(i); 
			if (field.getName().matches(parsed.getFields())) {
				availableFields.add(new DataFieldMetadataWrapper(inputPortIndex, i, field, parsed.getNamespace()));
			}
		}

		return availableFields;
	}
	
	@Override
	public void visit(Attribute element) throws Exception {
		WritableValue value = parseValue(element.getProperty(MappingProperty.VALUE));
		String name = element.getProperty(MappingProperty.NAME);
		ParsedName pName = parseName(name);
		
		boolean writeNull = Element.WRITE_NULL_DEFAULT;
		
		String writeNullString = element.getParent().getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
		String omitNullString = element.getParent().getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);
		if (writeNullString != null || omitNullString != null) {
			if (omitNullString == null) {
				writeNull = Arrays.asList(writeNullString.split(XmlMapping.DELIMITER)).contains(name);
			} else if (writeNullString == null) {
				writeNull = !Arrays.asList(omitNullString.split(XmlMapping.DELIMITER)).contains(name);
			} else {
				writeNull = Arrays.asList(writeNullString.split(XmlMapping.DELIMITER)).contains(name) 
					&& !Arrays.asList(omitNullString.split(XmlMapping.DELIMITER)).contains(name);
			}
		}
		
		WritableAttribute attribute = new WritableAttribute(pName.getName(), pName.getPrefix(),	value, writeNull);
		currentParent.addAttribute(attribute);
	}

	@Override
	public void visit(Element element) throws Exception {
		if (element.isTemplate()) {
			return;
		}
		if (isInRecursion()) {
			currentParent.addChild(compiledMap.get(element));
			return;
		}
		
		WritableElement previousParent = currentParent;
		WritableLoopElement previousLoopParent = currentLoopParent;
		
		Tag tag = tagMap.get(element);
		if (tag != null) {
			List<String> stringKeysList = null;
			List<String> stringParentKeysList = null;
			String filterExpression = null;

			boolean isHidden = Element.HIDE_DEFAULT;
			
			Relation info = element.getRelation();
			if (info != null) {
				String string = info.getProperty(MappingProperty.KEY);
				if (string != null) {
					stringKeysList = Arrays.asList(string.split(XmlMapping.DELIMITER));
				}
				string = info.getProperty(MappingProperty.PARENT_KEY);
				if (string != null) {
					stringParentKeysList = Arrays.asList(string.split(XmlMapping.DELIMITER));
				}
				filterExpression = info.getProperty(MappingProperty.FILTER);
				
				String isHiddenString = element.getProperty(MappingProperty.HIDE);
				if (isHiddenString != null) {
					isHidden = Boolean.parseBoolean(isHiddenString);
				}
			}
			
			PortData portData = portDataMap.get(tag.getPortIndex());			
			DataRecordMetadata metadata = portData.getInPort().getMetadata();
			
			int[] keys = null;
			if (stringKeysList != null) {
				keys = new int[stringKeysList.size()];			
				for (int i = 0; i < keys.length; i++) {
					keys[i] = metadata.getFieldPosition(stringKeysList.get(i));
				}
			}
			int[] parentKeys = null;
			if (stringParentKeysList != null && currentLoopParent != null) {
				parentKeys = new int[stringParentKeysList.size()];
				metadata = currentLoopParent.getRecord().getMetadata();
				for (int i = 0; i < parentKeys.length; i++) {
					parentKeys[i] = metadata.getFieldPosition(stringParentKeysList.get(i));
				}
			}
			ParsedName pName = parseName(element.getProperty(MappingProperty.NAME));
			
			WritableLoopElement loopElement =
				new WritableLoopElement(currentLoopParent, pName.getName(), pName.getPrefix(), isHidden,
						portData, currentLoopParent != null ? currentLoopParent.getPortIndex() : -1, keys, parentKeys,
						filterExpression, graph, componentId, logger);
			if (currentParent != null) {
				currentParent.addChild(loopElement);
			}
			availableData.push(tag.getPortIndex());
			addedPorts.add(element);
			
			compiledMap.put(element, loopElement);
			currentParent = currentLoopParent = loopElement;
			if (element == modelPartitionElement) {
				partitionElement = loopElement;
			}
		} else {
			ParsedName pName = parseName(element.getProperty(MappingProperty.NAME));
			boolean writeNull = Element.WRITE_NULL_DEFAULT;
			String writeNullString = element.getProperty(MappingProperty.WRITE_NULL_ELEMENT);
			if (writeNullString != null) {
				writeNull = Boolean.parseBoolean(writeNullString);
			}
			
			WritableElement writableElement = new WritableElement(pName.getName(), pName.getPrefix(), writeNull);
			if (currentParent != null) {
				currentParent.addChild(writableElement);
			}
			if (root == null) {
				root = writableElement;
			}
			compiledMap.put(element, writableElement);
			currentParent = writableElement;
		}
		for (Namespace namespace : element.getNamespaces()) {
			namespace.accept(this);
		}
		if (element.getWildcardAttribute() != null) {
			element.getWildcardAttribute().accept(this);
		}
		for (Attribute attribute : element.getAttributes()) {
			attribute.accept(this);
		}
		for (AbstractElement subElement : element.getChildren()) {
			subElement.accept(this);
		}
		
		currentParent = previousParent;
		currentLoopParent = previousLoopParent;
		
		if (addedPorts.contains(element)) {
			availableData.pop();
		}
	}

	@Override
	public void visit(Namespace element) throws Exception {
		if (!XmlMapping.MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(element.getProperty(MappingProperty.VALUE))) {
			WritableNamespace namespace = new WritableNamespace(element.getProperty(MappingProperty.NAME),
					element.getProperty(MappingProperty.VALUE));
			currentParent.addNamespace(namespace);
		}
	}

	@Override
	public void visit(Value element) throws Exception {
		WritableValue value = parseValue(element.getProperty(MappingProperty.VALUE));
		currentParent.addChild(value);
	}
	
	private WritableValue parseValue(String inputValue) {
		if (inputValue == null) {
			return WritableValue.newInstance(new TextValue[0]);
		}
		
		List<TextValue> value = new LinkedList<TextValue>();
		String valueToProcess = inputValue.trim();
		
		Matcher matcher = XmlMapping.DATA_REFERENCE.matcher(valueToProcess);
		String field;
		String portName;
		int portIndex;
		String fieldName;
		Integer delimiterIndex;
		Integer inputPortIndex;
		
		int processed = 0;
		
		while (matcher.find()) {
			if (matcher.start() > processed) {
				String staticValue = valueToProcess.substring(processed, matcher.start());
				staticValue = staticValue.replaceAll(XmlMapping.ESCAPED_PORT_REGEX, XmlMapping.PORT_IDENTIFIER);
				value.add(new StaticValue(staticValue));
			}
			field = valueToProcess.substring(matcher.start(), matcher.end());
			if (field.charAt(0) == '{') {
				field = field.substring(1, field.length() - 1);
			}
			delimiterIndex = field.indexOf('.');
			portName = field.substring(1, delimiterIndex);
			fieldName = field.substring(delimiterIndex + 1);
			try {
				portIndex = Integer.valueOf(portName);
				inputPortIndex = getFirstLocalPortIndex(portName, availableData, inPorts);
				DynamicValue dynValue = new DynamicValue(portIndex, inPorts.get(inputPortIndex).getFieldPosition(fieldName));
				value.add(dynValue);
			} catch (NumberFormatException ex) {
				for (Integer inPortIndex : availableData) {
					DataRecordMetadata recordMetadata = inPorts.get(inPortIndex); 
					if (recordMetadata.getName().equals(portName)) {
						DynamicValue dynValue = new DynamicValue(inPortIndex, recordMetadata.getFieldPosition(fieldName));
						value.add(dynValue);
						break;
					}
				}
			}
			processed = matcher.end();
		}
		if (processed < valueToProcess.length()) {
			String staticValue = valueToProcess.substring(processed);
			staticValue = staticValue.replaceAll(XmlMapping.ESCAPED_PORT_REGEX, XmlMapping.PORT_IDENTIFIER);
			value.add(new StaticValue(staticValue));
		}
		return WritableValue.newInstance(value.toArray(new TextValue[value.size()]));
	}
	
	private static class ParsedName {
		private final String prefix;
		private final String name;
		
		public ParsedName(String prefix, String name) {
			this.prefix = prefix;
			this.name = name;
		}

		public String getPrefix() {
			return prefix;
		}

		public String getName() {
			return name;
		}
	}
	
	public static ParsedName parseName(String name) {
		int index = name != null ? name.indexOf(':') : -1;
		if (index != -1) {
			return new ParsedName(name.substring(0, index), name.substring(index + 1));
		} else {
			return new ParsedName(null, name);
		}
	}
	
	@Override
	public void visit(Comment element) throws Exception {
		if (Boolean.valueOf(element.getProperty(MappingProperty.INCLUDE))) { 
			WritableValue value = parseValue(element.getProperty(MappingProperty.VALUE));
			currentParent.addChild(new WritableComment(value));
		}
	}
}

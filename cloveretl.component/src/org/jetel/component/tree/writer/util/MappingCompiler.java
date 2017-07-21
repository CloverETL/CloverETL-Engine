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
package org.jetel.component.tree.writer.util;

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
import org.jetel.component.RecordFilter;
import org.jetel.component.RecordFilterFactory;
import org.jetel.component.tree.writer.model.design.AbstractNode;
import org.jetel.component.tree.writer.model.design.Attribute;
import org.jetel.component.tree.writer.model.design.CDataSection;
import org.jetel.component.tree.writer.model.design.CollectionNode;
import org.jetel.component.tree.writer.model.design.Comment;
import org.jetel.component.tree.writer.model.design.ContainerNode;
import org.jetel.component.tree.writer.model.design.MappingProperty;
import org.jetel.component.tree.writer.model.design.Namespace;
import org.jetel.component.tree.writer.model.design.ObjectNode;
import org.jetel.component.tree.writer.model.design.Relation;
import org.jetel.component.tree.writer.model.design.TreeWriterMapping;
import org.jetel.component.tree.writer.model.design.Value;
import org.jetel.component.tree.writer.model.design.WildcardAttribute;
import org.jetel.component.tree.writer.model.design.WildcardNode;
import org.jetel.component.tree.writer.model.runtime.DynamicValue;
import org.jetel.component.tree.writer.model.runtime.NodeValue;
import org.jetel.component.tree.writer.model.runtime.PortBinding;
import org.jetel.component.tree.writer.model.runtime.StaticValue;
import org.jetel.component.tree.writer.model.runtime.WritableAttribute;
import org.jetel.component.tree.writer.model.runtime.WritableCData;
import org.jetel.component.tree.writer.model.runtime.WritableCollection;
import org.jetel.component.tree.writer.model.runtime.WritableComment;
import org.jetel.component.tree.writer.model.runtime.WritableContainer;
import org.jetel.component.tree.writer.model.runtime.WritableMapping;
import org.jetel.component.tree.writer.model.runtime.WritableNamespace;
import org.jetel.component.tree.writer.model.runtime.WritableObject;
import org.jetel.component.tree.writer.model.runtime.WritableValue;
import org.jetel.component.tree.writer.portdata.PortData;
import org.jetel.component.tree.writer.util.MappingTagger.Tag;
import org.jetel.component.tree.writer.xml.XmlMappingValidator;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Mapping compiler, which processes xml mapping and generates engine model. Assumes that xml mapping is valid!
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 15 Dec 2010
 */
public class MappingCompiler extends AbstractVisitor {

	private static final String FILTER_PREFIX = "//#CTL2\n";

	private final Map<Integer, DataRecordMetadata> inPorts;
	private TransformationGraph graph;
	private String componentId;
	private Log logger;

	private Map<Integer, PortData> portDataMap;
	private Map<ContainerNode, Tag> tagMap;

	private Map<ObjectNode, WritableContainer> compiledMap = new HashMap<ObjectNode, WritableContainer>();
	private Stack<Integer> availableData = new Stack<Integer>();
	private Set<ContainerNode> addedPorts = new HashSet<ContainerNode>();

	private ContainerNode modelPartitionElement;
	private WritableContainer partitionElement;

	private WritableContainer root;
	private WritableContainer currentParent;
	private WritableContainer currentLoopParent;

	private MappingTagger tagger;

	public MappingCompiler(Map<Integer, DataRecordMetadata> inPorts, String sortHintsString, boolean singleTopLevelRecord, boolean inMemoryCache) {
		this.inPorts = inPorts;
		this.tagger = new MappingTagger(inPorts, sortHintsString, singleTopLevelRecord, inMemoryCache);
	}

	public WritableMapping compile(Map<Integer, InputPort> inPorts, boolean partition)
			throws ComponentNotReadyException {
		tagger.setResolvePartition(partition);
		tagger.tag();

		tagMap = tagger.getTagMap();
		portDataMap = tagger.getPortDataMap(inPorts);
		modelPartitionElement = tagger.getPartitionElement();

		mapping.visit(this);
		return new WritableMapping(root, partitionElement);
	}

	public static int resolvePartitionKeyPortIndex(TreeWriterMapping mapping, Map<Integer, DataRecordMetadata> inPorts) {
		MappingTagger tagger = new MappingTagger(inPorts, null, false, false);
		tagger.setResolvePartition(true);
		tagger.setMapping(mapping);
		tagger.tag();
		return tagger.getPartitionElementPortIndex();
	}

	@Override
	public void setMapping(TreeWriterMapping mapping) {
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
	public void visit(WildcardAttribute element) throws Exception {
		String include = element.getProperty(MappingProperty.INCLUDE);
		String exclude = element.getProperty(MappingProperty.EXCLUDE);
		List<DataFieldMetadataWrapper> availableFields = gatherAvailableFields(include, exclude, element.getParent());

		String writeNull = element.getParent().getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
		String omitNull = element.getParent().getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);
		Set<DataFieldMetadataWrapper> writeNullSet = gatherNullSet(writeNull, omitNull, element.getParent());

		for (DataFieldMetadataWrapper dataFieldWrapper : availableFields) {
			WritableValue name = WritableValue.newInstance(new StaticValue(dataFieldWrapper.dataFieldMetadata.getName()));
			WritableValue namespace = WritableValue.newInstance(new StaticValue(dataFieldWrapper.namespace));
			WritableValue value = WritableValue.newInstance(new NodeValue[] { new DynamicValue(dataFieldWrapper.port, dataFieldWrapper.fieldIndex, dataFieldWrapper.dataFieldMetadata.getContainerType()) });
			WritableAttribute attribute = new WritableAttribute(name, namespace, value, writeNullSet.contains(dataFieldWrapper));
			currentParent.addAttribute(attribute);
		}
	}

	@Override
	public void visit(WildcardNode element) throws Exception {
		String include = element.getProperty(MappingProperty.INCLUDE);
		String exclude = element.getProperty(MappingProperty.EXCLUDE);
		List<DataFieldMetadataWrapper> availableFields = gatherAvailableFields(include, exclude, element.getParent());

		String writeNull = element.getProperty(MappingProperty.WRITE_NULL_ELEMENT);
		String omitNull = element.getProperty(MappingProperty.OMIT_NULL_ELEMENT);
		String dataType = element.getProperty(MappingProperty.DATA_TYPE);
		Set<DataFieldMetadataWrapper> writeNullSet = gatherNullSet(writeNull, omitNull, element.getParent());

		for (DataFieldMetadataWrapper dataFieldWrapper : availableFields) {
			WritableValue name = WritableValue.newInstance(new StaticValue(dataFieldWrapper.dataFieldMetadata.getName()));
			WritableValue namespace = WritableValue.newInstance(new StaticValue(dataFieldWrapper.namespace));
			WritableValue value = WritableValue.newInstance(new NodeValue[] { new DynamicValue(dataFieldWrapper.port, dataFieldWrapper.fieldIndex, dataFieldWrapper.dataFieldMetadata.getContainerType()) });
			WritableObject subNode = new WritableObject(name, namespace, writeNullSet.contains(dataFieldWrapper), false, dataType);
			subNode.addChild(value);
			currentParent.addChild(subNode);
		}
	}

	private List<DataFieldMetadataWrapper> gatherAvailableFields(String include, String exclude, ContainerNode container) {
		List<DataFieldMetadataWrapper> availableFields = new LinkedList<DataFieldMetadataWrapper>();

		if (include != null) {
			for (String aggregateExpression : include.split(TreeWriterMapping.DELIMITER)) {
				if (aggregateExpression.matches(XmlMappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN)) {
					availableFields.addAll(getFields(aggregateExpression, container));
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
			for (String aggregateExpression : exclude.split(TreeWriterMapping.DELIMITER)) {
				if (aggregateExpression.matches(XmlMappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN)) {
					availableFields.removeAll(getFields(aggregateExpression, container));
				}
			}
		}

		return availableFields;
	}

	private Set<DataFieldMetadataWrapper> gatherNullSet(String writeNull, String omitNull, ContainerNode container) {
		Set<DataFieldMetadataWrapper> writeNullSet = new HashSet<DataFieldMetadataWrapper>();
		if (writeNull != null) {
			for (String aggregateExpression : writeNull.split(TreeWriterMapping.DELIMITER)) {
				if (aggregateExpression.matches(XmlMappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN)) {
					writeNullSet.addAll(getFields(aggregateExpression, container));
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
			for (String aggregateExpression : omitNull.split(TreeWriterMapping.DELIMITER)) {
				if (aggregateExpression.matches(XmlMappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN)) {
					writeNullSet.removeAll(getFields(aggregateExpression, container));
				}
			}
		}

		return writeNullSet;
	}

	private Set<DataFieldMetadataWrapper> getFields(String aggregateExpression, ContainerNode parent) {
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

		boolean writeNull = ObjectNode.WRITE_NULL_DEFAULT;

		String writeNullString = element.getParent().getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
		String omitNullString = element.getParent().getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);
		if (writeNullString != null || omitNullString != null) {
			if (omitNullString == null) {
				writeNull = Arrays.asList(writeNullString.split(TreeWriterMapping.DELIMITER)).contains(name);
			} else if (writeNullString == null) {
				writeNull = !Arrays.asList(omitNullString.split(TreeWriterMapping.DELIMITER)).contains(name);
			} else {
				writeNull = Arrays.asList(writeNullString.split(TreeWriterMapping.DELIMITER)).contains(name) && !Arrays.asList(omitNullString.split(TreeWriterMapping.DELIMITER)).contains(name);
			}
		}

		// TODO writeNull should be evaluated later; at the time of writing, when the actual attribute name is known
		WritableAttribute attribute = new WritableAttribute(pName.getNameValue(), pName.getPrefixValue(), value, writeNull);
		currentParent.addAttribute(attribute);
	}

	@Override
	public void visit(ObjectNode element) throws Exception {
		if (element.isTemplate()) {
			return;
		}
		if (isInRecursion()) {
			currentParent.addChild(compiledMap.get(element));
			return;
		}

		WritableContainer previousParent = currentParent;
		WritableContainer previousLoopParent = currentLoopParent;

		Tag tag = tagMap.get(element);
		if (tag != null) {
			availableData.push(tag.getPortIndex());
			addedPorts.add(element);
		}
		
		WritableObject writableNode;
		ParsedName pName = parseName(element.getProperty(MappingProperty.NAME));
		WritableValue name = pName.getNameValue();
		WritableValue namespace = pName.getPrefixValue();
		boolean isHidden = ObjectNode.HIDE_DEFAULT;
		String isHiddenString = element.getProperty(MappingProperty.HIDE);
		if (isHiddenString != null) {
			isHidden = Boolean.parseBoolean(isHiddenString);
		}
		String dataType = element.getProperty(MappingProperty.DATA_TYPE);

		if (tag != null) {
			PortBinding portBinding = compilePortBinding(element, tag);
			writableNode = new WritableObject(name, namespace, isWriteNull(element), portBinding, isHidden, element.getParent() == null, dataType);
			if (currentParent != null) {
				currentParent.addChild(writableNode);
			}
			currentParent = currentLoopParent = writableNode;
			if (element == modelPartitionElement) {
				partitionElement = writableNode;
			}
		} else {
			writableNode = new WritableObject(name, namespace, isWriteNull(element), isHidden, element.getParent() == null, dataType);
			if (currentParent != null) {
				currentParent.addChild(writableNode);
			}
			if (root == null) {
				root = writableNode;
			}
			currentParent = writableNode;
		}

		compiledMap.put(element, writableNode);

		for (Namespace ns : element.getNamespaces()) {
			ns.accept(this);
		}
		if (element.getWildcardAttribute() != null) {
			element.getWildcardAttribute().accept(this);
		}
		for (Attribute attribute : element.getAttributes()) {
			attribute.accept(this);
		}
		for (AbstractNode subNode : element.getChildren()) {
			subNode.accept(this);
		}

		currentParent = previousParent;
		currentLoopParent = previousLoopParent;

		if (addedPorts.contains(element)) {
			availableData.pop();
		}
	}

	@Override
	public void visit(CollectionNode collection) throws Exception {
		WritableContainer previousParent = currentParent;
		WritableContainer previousLoopParent = currentLoopParent;

		WritableCollection writableContainer;
		ParsedName pName = parseName(collection.getProperty(MappingProperty.NAME));
		WritableValue name = parseValue(pName.getName());
		WritableValue namespace = parseValue(pName.getPrefix());
		Tag tag = tagMap.get(collection);
		if (tag != null) {
			PortBinding portBinding = compilePortBinding(collection, tag);
			writableContainer = new WritableCollection(name, namespace, isWriteNull(collection), portBinding);
			if (currentParent != null) {
				currentParent.addChild(writableContainer);
			}
			availableData.push(tag.getPortIndex());
			addedPorts.add(collection);
			currentParent = currentLoopParent = writableContainer;
			if (collection == modelPartitionElement) {
				partitionElement = writableContainer;
			}

		} else {
			writableContainer = new WritableCollection(name, namespace, isWriteNull(collection));
			if (currentParent != null) {
				currentParent.addChild(writableContainer);
			}
			if (root == null) {
				root = writableContainer;
			}
			currentParent = writableContainer;
		}

		for (AbstractNode subNode : collection.getChildren()) {
			subNode.accept(this);
		}

		currentParent = previousParent;
		currentLoopParent = previousLoopParent;

		if (addedPorts.contains(collection)) {
			availableData.pop();
		}
	}

	private boolean isWriteNull(ContainerNode container) {
		String writeNullString = container.getProperty(MappingProperty.WRITE_NULL_ELEMENT);
		if (writeNullString != null) {
			return Boolean.parseBoolean(writeNullString);
		}

		return ObjectNode.WRITE_NULL_DEFAULT;
	}

	private PortBinding compilePortBinding(ContainerNode container, Tag tag) throws ComponentNotReadyException {
		List<String> stringKeysList = null;
		List<String> stringParentKeysList = null;
		String filterExpression = null;

		Relation info = container.getRelation();
		if (info != null) {
			String string = info.getProperty(MappingProperty.KEY);
			if (string != null) {
				stringKeysList = Arrays.asList(string.split(TreeWriterMapping.DELIMITER));
			}
			string = info.getProperty(MappingProperty.PARENT_KEY);
			if (string != null) {
				stringParentKeysList = Arrays.asList(string.split(TreeWriterMapping.DELIMITER));
			}
			filterExpression = info.getProperty(MappingProperty.FILTER);
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
			metadata = currentLoopParent.getPortBinding().getRecord().getMetadata();
			for (int i = 0; i < parentKeys.length; i++) {
				parentKeys[i] = metadata.getFieldPosition(stringParentKeysList.get(i));
			}
		}

		RecordFilter recordFilter = null;
		if (filterExpression != null) {
			recordFilter = RecordFilterFactory.createFilter(FILTER_PREFIX + filterExpression, metadata, graph, componentId, logger);
		}

		return new PortBinding(currentLoopParent != null ? currentLoopParent.getPortBinding() : null, portData, keys, parentKeys, recordFilter);
	}

	@Override
	public void visit(Namespace element) throws Exception {
		if (!TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(element.getProperty(MappingProperty.VALUE))) {
			WritableNamespace namespace = new WritableNamespace(element.getProperty(MappingProperty.NAME), element.getProperty(MappingProperty.VALUE));
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
			return WritableValue.newInstance(new NodeValue[0]);
		}

		List<NodeValue> value = new LinkedList<NodeValue>();
		String valueToProcess = inputValue.trim();

		Matcher matcher = TreeWriterMapping.DATA_REFERENCE.matcher(valueToProcess);
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
				staticValue = staticValue.replaceAll(TreeWriterMapping.ESCAPED_PORT_REGEX, TreeWriterMapping.PORT_IDENTIFIER);
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
				DynamicValue dynValue = new DynamicValue(portIndex, inPorts.get(inputPortIndex).getFieldPosition(fieldName),
						inPorts.get(inputPortIndex).getField(fieldName).getContainerType());
				value.add(dynValue);
			} catch (NumberFormatException ex) {
				for (Integer inPortIndex : availableData) {
					DataRecordMetadata recordMetadata = inPorts.get(inPortIndex);
					if (recordMetadata.getName().equals(portName)) {
						DynamicValue dynValue = new DynamicValue(inPortIndex, recordMetadata.getFieldPosition(fieldName),
							recordMetadata.getField(fieldName).getContainerType());
						value.add(dynValue);
						break;
					}
				}
			}
			processed = matcher.end();
		}
		if (processed < valueToProcess.length()) {
			String staticValue = valueToProcess.substring(processed);
			staticValue = staticValue.replaceAll(TreeWriterMapping.ESCAPED_PORT_REGEX, TreeWriterMapping.PORT_IDENTIFIER);
			value.add(new StaticValue(staticValue));
		}
		return WritableValue.newInstance(value.toArray(new NodeValue[value.size()]));
	}

	private class ParsedName {
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

		public WritableValue getPrefixValue() {
			return prefix != null ? parseValue(prefix) : null;
		}

		public WritableValue getNameValue() {
			return parseValue(name);
		}
	}

	public ParsedName parseName(String name) {
		int index = name != null ? name.indexOf(':') : -1;
		if (index != -1) {
			return new ParsedName(name.substring(0, index), name.substring(index + 1));
		} else {
			return new ParsedName(null, name);
		}
	}

	@Override
	public void visit(Comment element) throws Exception {
		if (Boolean.valueOf(element.getProperty(MappingProperty.WRITE))) {
			WritableValue value = parseValue(element.getProperty(MappingProperty.VALUE));
			currentParent.addChild(new WritableComment(value));
		}
	}
	
	@Override
	public void visit(CDataSection cdataSection) throws Exception {
		WritableValue value = parseValue(cdataSection.getProperty(MappingProperty.VALUE));
		currentParent.addChild(new WritableCData(value));
	}
}

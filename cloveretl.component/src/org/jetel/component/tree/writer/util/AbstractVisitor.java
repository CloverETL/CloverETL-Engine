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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.regex.Matcher;

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
import org.jetel.component.tree.writer.model.design.TemplateEntry;
import org.jetel.component.tree.writer.model.design.TreeWriterMapping;
import org.jetel.component.tree.writer.model.design.Value;
import org.jetel.component.tree.writer.model.design.WildcardAttribute;
import org.jetel.component.tree.writer.model.design.WildcardNode;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This visitor handles referencing templates, simulates expanding and prevents infinite loops when templates are used in recursive way.  
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27 Jan 2011
 */
public abstract class AbstractVisitor implements MappingVisitor {
	
	private Stack<TemplateEntry> templateStack = new Stack<TemplateEntry>();
	private TemplateEntry recursionStart = null;
	
	protected TreeWriterMapping mapping;
	
	@Override
	public void visit(WildcardAttribute element) throws Exception {}
	@Override
	public void visit(WildcardNode element) throws Exception {}
	@Override
	public void visit(Attribute element) throws Exception {}
	@Override
	public void visit(ObjectNode element) throws Exception {}
	@Override
	public void visit(Namespace element) throws Exception {}
	@Override
	public void visit(Value element) throws Exception {}
	@Override
	public void visit(Relation element) throws Exception {}
	@Override
	public void visit(Comment element) throws Exception {}
	@Override
	public void visit(CDataSection cdataSection) throws Exception {}
	@Override
	public void visit(CollectionNode element) throws Exception {}
	
	@Override
	public void visit(TemplateEntry objectTemplateEntry) throws Exception {
		if (recursionStart == objectTemplateEntry) {
			return;
		}
		
		String templateKey = objectTemplateEntry.getProperty(MappingProperty.NAME);
		if (templateKey == null) {
			return;
		}
		
		if (recursionStart == null) {
			if (templateStack.contains(objectTemplateEntry)) {
				recursionStart = objectTemplateEntry;
			}
		}
		
		templateStack.push(objectTemplateEntry);
		ObjectNode template = mapping.getTemplates().get(templateKey);
		if (template == null) {
			return;
		}
		
		for (AbstractNode child : template.getChildren()) {
			child.accept(this);
		}
		templateStack.pop();
		recursionStart = null;
	}
	
	protected void visitObject(ObjectNode element) throws Exception {
		for (Namespace namespace : element.getNamespaces()) {
			namespace.accept(this);
		}
		if (element.getWildcardAttribute() != null) {
			element.getWildcardAttribute().accept(this);
		}
		for (Attribute attribute : element.getAttributes()) {
			attribute.accept(this);
		}
		visitChildren(element);
	}
	
	protected void visitChildren(ContainerNode element) throws Exception {
		if (element.getRelation() != null) {
			element.getRelation().accept(this);
		}
		for (AbstractNode child : element.getChildren()) {
			child.accept(this);
		}
	}
	
	protected ContainerNode getRecurringParent(ContainerNode element) {
		if (element.getParent() != null) {
			return getRecurringParentImpl(element.getParent());
		}
		return null;
	}
	
	private ContainerNode getRecurringParentImpl(ContainerNode element) {
		if (element instanceof ObjectNode && ((ObjectNode) element).isTemplate()) {
			TemplateEntry entry = templateStack.pop();
			ContainerNode recurringElement = getRecurringParentImpl(entry.getParent());
			templateStack.push(entry);
			return recurringElement;
		}
		if (element.getRelation() != null) {
			return element;
		} else if (element.getParent() != null){
			return getRecurringParentImpl(element.getParent());
		}
		
		return null;
	}

	public boolean isInRecursion() {
		return recursionStart != null;
	}

	public void setMapping(TreeWriterMapping mapping) {
		this.mapping = mapping;
	}
	
	/**
	 * @return the mapping
	 */
	public TreeWriterMapping getMapping() {
		return mapping;
	}
	
	protected Integer getFirstPortIndex(String inPortString, Map<Integer, DataRecordMetadata> availablePorts) {
		if (inPortString == null) {
			return null;
		}
		try {
			Integer parsedIndex = Integer.valueOf(inPortString);
			if (availablePorts.containsKey(parsedIndex)) {
				return parsedIndex;
			}	
		} catch (NumberFormatException ex) {
			for (Entry<Integer, DataRecordMetadata> entry : availablePorts.entrySet()) {
				if (entry.getValue().getName().equals(inPortString)) {
					return entry.getKey();
				}
			}
		}
		return null;
	}
	
	protected Integer getFirstLocalPortIndex(String inPortString, Collection<Integer> localAvailablePorts, Map<Integer, DataRecordMetadata> globalAvailablePorts) {
		if (inPortString == null) {
			return null;
		}
		try {
			Integer parsedIndex = Integer.valueOf(inPortString);
			for (Integer index : localAvailablePorts) {
				if (index.equals(parsedIndex)) {
					return parsedIndex;
				}
			}	
		} catch (NumberFormatException ex) {
			for (Integer localIndex : localAvailablePorts) {
				DataRecordMetadata metadata = globalAvailablePorts.get(localIndex);
				if (metadata.getName().equals(inPortString)) {
					return localIndex;
				}
			}
		}
		return null;
	}
	
	protected List<ParsedFieldExpression> parseValueExpression(String valueExpression) {
		List<ParsedFieldExpression> toReturn = new LinkedList<ParsedFieldExpression>();
		if (valueExpression == null) {
			return toReturn;
		}
		
		Matcher matcher = TreeWriterMapping.DATA_REFERENCE.matcher(valueExpression);
		String field;
		String portName;
		String fieldName;
		Integer delimiterIndex;
		
		while (matcher.find()) {
			field = valueExpression.substring(matcher.start(), matcher.end());
			if (field.charAt(0) == '{') {
				field = field.substring(1, field.length() - 1);
			}
			delimiterIndex = field.indexOf('.');
			portName = field.substring(1, delimiterIndex);
			fieldName = field.substring(delimiterIndex + 1);
			toReturn.add(new ParsedFieldExpression(null, portName, fieldName));
		}
		
		return toReturn;
	}
	
	protected ParsedFieldExpression parseAggregateExpression(String aggregateExpression) {
		int namespacePos = aggregateExpression.indexOf(':');
		String namespace = null;
		if (namespacePos != -1) {
			namespace = aggregateExpression.substring(0, namespacePos);
		} 

		int fieldsPos = aggregateExpression.indexOf('.');
		String port = aggregateExpression.substring(namespacePos + 2, fieldsPos); //+:$
		String fields = aggregateExpression.substring(fieldsPos + 1).replaceAll("\\*", ".*");
		
		return new ParsedFieldExpression(namespace, port, fields);
	}
	
	public static class ParsedFieldExpression {
		
		String namespace;
		String port;
		String fields;
		
		private ParsedFieldExpression(String namespace, String port, String fields) {
			this.namespace = namespace;
			this.port = port;
			this.fields = fields;
		}

		public String getNamespace() {
			return namespace;
		}

		public String getPort() {
			return port;
		}

		public String getFields() {
			return fields;
		}
	}
}

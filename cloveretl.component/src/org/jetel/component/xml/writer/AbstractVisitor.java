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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.regex.Matcher;

import org.jetel.component.xml.writer.mapping.MappingProperty;
import org.jetel.component.xml.writer.mapping.ObjectAggregate;
import org.jetel.component.xml.writer.mapping.ObjectAttribute;
import org.jetel.component.xml.writer.mapping.ObjectComment;
import org.jetel.component.xml.writer.mapping.ObjectElement;
import org.jetel.component.xml.writer.mapping.ObjectNamespace;
import org.jetel.component.xml.writer.mapping.ObjectRepresentation;
import org.jetel.component.xml.writer.mapping.ObjectTemplateEntry;
import org.jetel.component.xml.writer.mapping.ObjectValue;
import org.jetel.component.xml.writer.mapping.RecurringElementInfo;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author LKREJCI (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27 Jan 2011
 */
public abstract class AbstractVisitor implements MappingVisitor {
	
	private Stack<ObjectTemplateEntry> templateStack = new Stack<ObjectTemplateEntry>();
	private ObjectTemplateEntry recursionStart = null;
	
	protected Mapping mapping;
	
	public void visit(ObjectAggregate element) throws Exception {}
	public void visit(ObjectAttribute element) throws Exception {}
	public void visit(ObjectElement element) throws Exception {}
	public void visit(ObjectNamespace element) throws Exception {}
	public void visit(ObjectValue element) throws Exception {}
	public void visit(RecurringElementInfo element) throws Exception {}
	public void visit(ObjectComment element) throws Exception {}
	
	public void visit(ObjectTemplateEntry objectTemplateEntry) throws Exception {
		if (recursionStart == objectTemplateEntry) {
			return;
		}
		
		String templateKey = objectTemplateEntry.getProperty(MappingProperty.TEMPLATE_NAME);
		if (templateKey == null) {
			return;
		}
		
		if (recursionStart == null) {
			if (templateStack.contains(objectTemplateEntry)) {
				recursionStart = objectTemplateEntry;
			}
		}
		
		templateStack.push(objectTemplateEntry);
		ObjectElement template = mapping.getTemplates().get(templateKey);
		if (template == null) {
			return;
		}
		
		for (ObjectRepresentation child : template.getChildren()) {
			child.accept(this);
		}
		templateStack.pop();
		recursionStart = null;
	}
	
	protected void visitChildren(ObjectElement element) throws Exception {
		for (ObjectNamespace namespace : element.getNamespaces()) {
			namespace.accept(this);
		}
		if (element.getAttributeInfo() != null) {
			element.getAttributeInfo().accept(this);
		}
		for (ObjectAttribute attribute : element.getAttributes()) {
			attribute.accept(this);
		}
		if (element.getRecurringInfo() != null) {
			element.getRecurringInfo().accept(this);
		}
		for (ObjectRepresentation child : element.getChildren()) {
			child.accept(this);
		}
	}
	
	protected ObjectElement getRecurringParent(ObjectElement element) {
		if (element.getParent() != null) {
			return getRecurringParentImpl(element.getParent());
		}
		return null;
	}
	
	private ObjectElement getRecurringParentImpl(ObjectElement element) {
		if (element.isTemplate()) {
			ObjectTemplateEntry entry = templateStack.pop();
			ObjectElement recurringElement = getRecurringParentImpl(entry.getParent());
			templateStack.push(entry);
			return recurringElement;
		}
		if (element.getRecurringInfo() != null) {
			return element;
		} else if (element.getParent() != null){
			return getRecurringParentImpl(element.getParent());
		}
		
		return null;
	}

	public boolean isInRecursion() {
		return recursionStart != null;
	}

	public void setMapping(Mapping mapping) {
		this.mapping = mapping;
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
		
		Matcher matcher = Mapping.DATA_REFERENCE.matcher(valueExpression);
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
	
	protected static class ParsedFieldExpression {
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

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
import org.jetel.component.xml.writer.mapping.WildcardElement;
import org.jetel.component.xml.writer.mapping.Attribute;
import org.jetel.component.xml.writer.mapping.Comment;
import org.jetel.component.xml.writer.mapping.Element;
import org.jetel.component.xml.writer.mapping.Namespace;
import org.jetel.component.xml.writer.mapping.AbstractElement;
import org.jetel.component.xml.writer.mapping.TemplateEntry;
import org.jetel.component.xml.writer.mapping.Value;
import org.jetel.component.xml.writer.mapping.Relation;
import org.jetel.component.xml.writer.mapping.XmlMapping;
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
	
	protected XmlMapping mapping;
	
	public void visit(WildcardElement element) throws Exception {}
	public void visit(Attribute element) throws Exception {}
	public void visit(Element element) throws Exception {}
	public void visit(Namespace element) throws Exception {}
	public void visit(Value element) throws Exception {}
	public void visit(Relation element) throws Exception {}
	public void visit(Comment element) throws Exception {}
	
	public void visit(TemplateEntry objectTemplateEntry) throws Exception {
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
		Element template = mapping.getTemplates().get(templateKey);
		if (template == null) {
			return;
		}
		
		for (AbstractElement child : template.getChildren()) {
			child.accept(this);
		}
		templateStack.pop();
		recursionStart = null;
	}
	
	protected void visitChildren(Element element) throws Exception {
		for (Namespace namespace : element.getNamespaces()) {
			namespace.accept(this);
		}
		if (element.getWildcardAttribute() != null) {
			element.getWildcardAttribute().accept(this);
		}
		for (Attribute attribute : element.getAttributes()) {
			attribute.accept(this);
		}
		if (element.getRelation() != null) {
			element.getRelation().accept(this);
		}
		for (AbstractElement child : element.getChildren()) {
			child.accept(this);
		}
	}
	
	protected Element getRecurringParent(Element element) {
		if (element.getParent() != null) {
			return getRecurringParentImpl(element.getParent());
		}
		return null;
	}
	
	private Element getRecurringParentImpl(Element element) {
		if (element.isTemplate()) {
			TemplateEntry entry = templateStack.pop();
			Element recurringElement = getRecurringParentImpl(entry.getParent());
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

	public void setMapping(XmlMapping mapping) {
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
		
		Matcher matcher = XmlMapping.DATA_REFERENCE.matcher(valueExpression);
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

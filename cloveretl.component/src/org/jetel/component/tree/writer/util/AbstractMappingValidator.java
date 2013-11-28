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

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

import org.jetel.component.tree.writer.model.design.AbstractNode;
import org.jetel.component.tree.writer.model.design.Attribute;
import org.jetel.component.tree.writer.model.design.CDataSection;
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
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.11.2011
 */
public abstract class AbstractMappingValidator extends AbstractVisitor {

	protected Map<AbstractNode, Map<MappingProperty, SortedSet<MappingError>>> errorsMap = new HashMap<AbstractNode, Map<MappingProperty, SortedSet<MappingError>>>();
	protected Stack<Integer> availablePorts = new Stack<Integer>();
	protected Map<Integer, DataRecordMetadata> inPorts;
	protected boolean errors = false;
	protected int maxErrors = 50;
	protected int maxErrorsLimit = 100;
	protected int maxWarnings = 50;
	protected int errorsCount;
	protected int warningsCount;
	protected ObjectNode globalPartition = null;
	protected boolean runIt;
	
	protected AbstractMappingValidator(Map<Integer, DataRecordMetadata> inputPorts) {
		this.inPorts = inputPorts;
	}

	protected static final class SeverityComparator implements Comparator<MappingError> {
			
			public static final SeverityComparator INSTANCE = new SeverityComparator();
	
			@Override
			public int compare(MappingError me1, MappingError me2) {
				int me1severity = getSeverityNumber(me1.getSeverity());
				int me2severity = getSeverityNumber(me2.getSeverity());
				return me2severity - me1severity;
			}
			
			private int getSeverityNumber(Severity s) {
				if (s == Severity.ERROR) return 2;
				if (s == Severity.WARNING) return 1;
				return 0;
			}
		}

	public void validate() {
		clear();
		mapping.visit(this);
	}

	public void clear() {
		errors = false;
		globalPartition = null;
		errorsMap.clear();
		availablePorts.clear();
		runIt = true;
		errorsCount = 0;
		warningsCount = 0;
	}

	@Override
	public void setMapping(TreeWriterMapping mapping) {
		this.mapping = mapping;
		clear();
	}

	public boolean containsErrors() { 
		return errors;
	}

	public Map<AbstractNode, Map<MappingProperty, SortedSet<MappingError>>> getErrorsMap() {
		return errorsMap;
	}

	protected List<Integer> getPortIndexes(String inPortString, Map<Integer, DataRecordMetadata> availablePorts) {
		List<Integer> toReturn = new LinkedList<Integer>();
		if (inPortString == null) {
			return toReturn;
		}
		
		try {
			Integer parsedIndex = Integer.valueOf(inPortString);
			if (availablePorts.containsKey(parsedIndex)) {
				toReturn.add(parsedIndex);
			}
		} catch (NumberFormatException ex) {
			for (Entry<Integer, DataRecordMetadata> entry : availablePorts.entrySet()) {
				if (entry.getValue() != null && entry.getValue().getName().equals(inPortString)) {
					toReturn.add(entry.getKey());
				}
			}
		}
		return toReturn;
	}

	protected Integer getAvailableInputPort(String key, AbstractNode element, MappingProperty keyword) {
		Integer toReturn = null;
		try {
			Integer portIndex = Integer.valueOf(key);
			for (Integer inPortIndex : availablePorts) {
				if (inPortIndex.equals(portIndex)) {
					return portIndex;
				}
			}
		} catch (NumberFormatException ex) {
			for (Integer inPortIndex : availablePorts) {
				if (inPorts.get(inPortIndex).getName().matches(key)) {
					if (toReturn != null) {
						addProblem(element, keyword, new MappingError("Ambiguous port '" + key + "'", Severity.WARNING));
					} else {
						toReturn = inPortIndex;
					}
				}
			}
		}
		return toReturn;
	}

	protected void checkAvailableData(AbstractNode element, MappingProperty property, DataRecordMetadata metadata, String[] fieldNames) {
		if (metadata == null) {
			addProblem(element, property, new MappingError("Port metadata not available", Severity.ERROR));
		} else {
			for (String fieldName : fieldNames) {
				if (metadata.getField(fieldName) == null) {
					addProblem(element, property, new MappingError("Record '" + metadata.getName() + "' does not contain field '" + fieldName + "'", Severity.ERROR));
				}
			}
		}
	}

	protected void addProblem(AbstractNode element, MappingProperty property, MappingError error) {
		if (error.getSeverity() == Severity.ERROR) {
			errorsCount++;
			errors = true;
			if (errorsCount > maxErrors) {
				if (errorsCount > maxErrorsLimit) {
					runIt = false;
				}
				return;
			}
		} else {
			warningsCount++;
			if (warningsCount > maxWarnings) {
				return;
			}
		}
		
		Map<MappingProperty, SortedSet<MappingError>> elementErrors = errorsMap.get(element);
		SortedSet<MappingError> errorList;
		if (elementErrors == null) {
			errorList = new TreeSet<MappingError>(SeverityComparator.INSTANCE);
			elementErrors = new HashMap<MappingProperty, SortedSet<MappingError>>();
			elementErrors.put(property, errorList);
			errorsMap.put(element, elementErrors);
		} else {
			errorList = elementErrors.get(property);
			if (errorList == null) {
				errorList = new TreeSet<MappingError>(SeverityComparator.INSTANCE);
				elementErrors.put(property, errorList);
			}
		}
		
		errorList.add(error);
	}
	
	@Override
	public void visit(Attribute element) throws Exception {
		
		if (!runIt) {
			return;
		}
		validateAttribute(element);
	}
	
	protected abstract void validateAttribute(Attribute attribute);
	
	@Override
	public void visit(Comment element) throws Exception {
		
		if (!runIt) {
			return;
		}
		validateComment(element);
	}
	
	protected abstract void validateComment(Comment comment);
	
	@Override
	public void visit(CDataSection cdataSection) throws Exception {
		
		if (!runIt) {
			return;
		}
		validateCDataSection(cdataSection);
	}
	
	protected void validateCDataSection(CDataSection section) {}
	
	@Override
	public void visit(Relation element) throws Exception {
		if (!runIt) {
			return;
		}
		
		checkRelationPortAndKeyBinding(element);
		validateRelation(element);
	}

	protected void checkRelationPortAndKeyBinding(Relation element) {
		checkCloverNamespaceAvailable(element.getParent());

		String inPortString = element.getProperty(MappingProperty.INPUT_PORT);
		Integer inPortIndex = null;
		if (inPortString == null) {
			addProblem(element, MappingProperty.INPUT_PORT, new MappingError("Input port not specified!", Severity.ERROR));
			return;
		} else {
			inPortIndex = getAvailableInputPort(inPortString, element, MappingProperty.INPUT_PORT);
			if (inPortIndex == null) {
				addProblem(element, MappingProperty.INPUT_PORT, new MappingError("Input port '" + inPortString + "' is not connected!", Severity.ERROR));
				return;
			}
		}

		String keyString = element.getProperty(MappingProperty.KEY);
		String parentKeyString = element.getProperty(MappingProperty.PARENT_KEY);

		if (parentKeyString != null && keyString == null) {
			addProblem(element, MappingProperty.KEY, new MappingError(MappingProperty.KEY.getName() + " attribute not specified!", Severity.ERROR));
		}
		if (parentKeyString == null && keyString != null) {
			addProblem(element, MappingProperty.PARENT_KEY, new MappingError(MappingProperty.PARENT_KEY.getName() + " attribute not specified!", Severity.ERROR));
		}

		if (keyString != null) {
			String[] keyList = keyString.split(TreeWriterMapping.DELIMITER);
			checkAvailableData(element, MappingProperty.KEY, inPorts.get(inPortIndex), keyList);

			if (parentKeyString != null) {
				if (parentKeyString.split(TreeWriterMapping.DELIMITER).length != keyList.length) {
					addProblem(element, MappingProperty.KEY, new MappingError("Count of fields must match parent key field count", Severity.ERROR));
					addProblem(element, MappingProperty.PARENT_KEY, new MappingError("Count of fields must match key field count", Severity.ERROR));
				}
			}

		}
		if (parentKeyString != null) {
			inPortString = null;
			ContainerNode parent = getRecurringParent(element.getParent());
			if (parent != null) {
				inPortString = parent.getRelation().getProperty(MappingProperty.INPUT_PORT);
			}
			if (inPortString == null) {
				addProblem(element, MappingProperty.PARENT_KEY, new MappingError("No data for parent key fields!", Severity.ERROR));
			} else {
				inPortIndex = getAvailableInputPort(inPortString, element, MappingProperty.PARENT_KEY);
				if (inPortIndex == null) {
					addProblem(element, MappingProperty.PARENT_KEY, new MappingError("No data for parent key fields!", Severity.ERROR));
				} else {
					checkAvailableData(element, MappingProperty.PARENT_KEY, inPorts.get(inPortIndex), parentKeyString.split(TreeWriterMapping.DELIMITER));
				}
			}
		}
	}
	
	protected abstract void validateRelation(Relation element);
	
	@Override
	public void visit(ObjectNode element) throws Exception {
		
		if (!runIt || isInRecursion()) {
			return;
		}
		if (element.getParent() == null) {
			visitObject(element);
			return;
		}
		if (element.isTemplate()) {
			checkCloverNamespaceAvailable(element);
			String templateName = element.getProperty(MappingProperty.NAME);
			if (templateName == null) {
				addProblem(element, MappingProperty.NAME, new MappingError("Unspecified template name", Severity.ERROR));
			}
			return;
		}
		
		checkCorrectBooleanValue(element, MappingProperty.WRITE_NULL_ELEMENT);
		
		List<Integer> addedPorts = null;
		Relation recurringInfo = element.getRelation();
		if (recurringInfo != null) {
			String inPortString = recurringInfo.getProperty(MappingProperty.INPUT_PORT);
			if (inPortString != null) {
				addedPorts = getPortIndexes(inPortString, inPorts);
				if (addedPorts.size() > 1) {
					addProblem(recurringInfo, MappingProperty.INPUT_PORT, new MappingError("Ambiguous ports!", Severity.WARNING));
				}
				for (Integer inputPortIndex : addedPorts) {
					availablePorts.push(inputPortIndex);
				}
			}
		}
		
		String hideString = element.getProperty(MappingProperty.HIDE);
		if (hideString != null) {
			checkCorrectBooleanValue(element, MappingProperty.HIDE);
		}
		
		validateElement(element);
		
		visitObject(element);

		if (addedPorts != null) {
			for (int i = 0; i < addedPorts.size(); i++) {
				availablePorts.pop();
			}
		}
	}
	
	protected abstract void validateElement(ObjectNode element);
	
	@Override
	public void visit(Namespace element) throws Exception {
		
		if (!runIt) {
			return;
		}
		validateNamespace(element);
	}
	
	protected abstract void validateNamespace(Namespace element);
	
	@Override
	public void visit(TemplateEntry objectTemplateEntry) throws Exception {
		if (!runIt) {
			return;
		}
		checkCloverNamespaceAvailable(objectTemplateEntry.getParent());
		
		checkTemplateExistence(objectTemplateEntry);
		validateTemplateEntry(objectTemplateEntry);
		
		super.visit(objectTemplateEntry);
	}
	
	protected void checkTemplateExistence(TemplateEntry objectTemplateEntry) {
		String templateName = objectTemplateEntry.getProperty(MappingProperty.NAME);
		if (templateName == null || !mapping.getTemplates().containsKey(templateName)) {
			addProblem(objectTemplateEntry, MappingProperty.NAME, new MappingError("Unknown template", Severity.ERROR));
			return;
		}
	}
	
	protected abstract void validateTemplateEntry(TemplateEntry element);
	
	@Override
	public void visit(Value element) {
		
		if (!runIt) {
			return;
		}
		validateValue(element);
	}
	
	protected void validateValue(AbstractNode element) {
		String value = element.getProperty(MappingProperty.VALUE);
		if (value == null) {
			addProblem(element, MappingProperty.VALUE, new MappingError("Empty value", Severity.WARNING));
			return;
		}
		
		List<ParsedFieldExpression> fields = parseValueExpression(value);
		for (ParsedFieldExpression parsedFieldExpression : fields) {
			Integer inPortIndex = getAvailableInputPort(parsedFieldExpression.getPort(), element, MappingProperty.VALUE);
			if (inPortIndex == null) {
				addProblem(element, MappingProperty.INPUT_PORT, new MappingError("Input port '" + parsedFieldExpression.getPort() + "' is not available here!", Severity.ERROR));
			} else if (inPorts.get(inPortIndex) == null) {
				addProblem(element, MappingProperty.INPUT_PORT, new MappingError("Metadata of port '" + parsedFieldExpression.getPort() + "' not available", Severity.ERROR));
			} else if (inPorts.get(inPortIndex).getField(parsedFieldExpression.getFields()) == null) {
				addProblem(element, MappingProperty.VALUE,
						new MappingError("Field '" + parsedFieldExpression.getFields() + "' is not available.", Severity.ERROR));
			}
		}
	}
	
	@Override
	public void visit(WildcardNode element) throws Exception {
		
		if (!runIt) {
			return;
		}
		checkCloverNamespaceAvailable(element.getParent());
		validateWildCardNode(element);
	}
	
	protected abstract void validateWildCardNode(WildcardNode element);
	
	@Override
	public void visit(WildcardAttribute element) throws Exception {
		
		if (!runIt) {
			return;
		}
		checkCloverNamespaceAvailable(element.getParent());
		validateWildCardAttribute(element);
	}
	
	protected abstract void validateWildCardAttribute(WildcardAttribute element);

	/**
	 * Extract property from given element and check whether returned String is of boolean type (TRUE/FALSE or 
	 * additionally emptyString/null) otherwise add exception to user view window.
	 * @param element ObjectRepresentation
	 * @param property MappingProperty
	 */
	protected void checkCorrectBooleanValue(AbstractNode element, MappingProperty property) {
		String value = element.getProperty(property);
		if (StringUtils.isEmpty(value)) {
			return;
		}
		if (!Boolean.TRUE.toString().equalsIgnoreCase(value) && !Boolean.FALSE.toString().equalsIgnoreCase(value)) {
			addProblem(element, property, new MappingError("Attribute accepts only boolean type values (true/false)", Severity.ERROR));
		}
	}
	
	protected void checkCloverNamespaceAvailable(ContainerNode element) {
		if (!isCloverNamespaceAvailable(element)) {
			addProblem(element, MappingProperty.UNKNOWN, new MappingError("Clover namespace is not available!", Severity.ERROR));
		}
	}
	
	private boolean isCloverNamespaceAvailable(ContainerNode container) {
		if (container instanceof ObjectNode) {
			ObjectNode namespaceSupportingContainer = (ObjectNode) container; 
		
			for (Namespace namespace : namespaceSupportingContainer.getNamespaces()) {
				if (TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI.equals(namespace.getProperty(MappingProperty.VALUE))) {
					return true;
				}
			}
		}
		
		if (container.getParent() != null) {
			return isCloverNamespaceAvailable(container.getParent());
		}
		return false;
	}
	
	/**
	 * Sets number of full error messages to collect. If exceeded, only error counter gets increased.
	 * Ensures: maxErrors <= maxErrorsLimit
	 * @param maxErrors
	 */
	public void setMaxErrors(int maxErrors) {
		if (maxErrors > maxErrorsLimit) {
			maxErrorsLimit = maxErrors;
		}
		this.maxErrors = maxErrors;
	}

	public int getMaxErrors() {
		return maxErrors;
	}

	/**
	 * Sets validation error count threshold. If exceeded, validation is interrupted
	 * and {@link #isValidationComplete()} returns <code>false</code>.
	 * Ensures: maxErrors <= maxErrorsLimit
	 * @param maxErrorsLimit
	 */
	public void setMaxErrorsLimit(int maxErrorsLimit) {
		if (maxErrorsLimit < maxErrors) {
			maxErrors = maxErrorsLimit;
		}
		this.maxErrorsLimit = maxErrorsLimit;
	}

	public int getMaxErrorsLimit() {
		return maxErrorsLimit;
	}

	/**
	 * Sets number of full warning messages to collect. If exceeded, only warnings counter gets increased.
	 */
	public void setMaxWarnings(int maxWarnings) {
		this.maxWarnings = maxWarnings;
	}

	public int getMaxWarnings() {
		return maxWarnings;
	}

	public int getErrorsCount() {
		return errorsCount;
	}

	public int getWarningsCount() {
		return warningsCount;
	}

	/**
	 * Indicates whether last {@link #validate()} processed whole mapping. 
	 * @return <code>false</code> if there were more validation errors then {@link #getMaxErrorsLimit()},
	 * in which case only a part of mapping was validated.
	 */
	public boolean isValidationComplete() {
		return runIt;
	}
}
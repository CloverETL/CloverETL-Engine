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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

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
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 15 Dec 2010
 */
public class MappingValidator extends AbstractVisitor {

	private final static String INPORT_REFERENCE_PATTERN = "(" + StringUtils.OBJECT_NAME_PATTERN + "|[0-9]+)";
	public final static String QUALIFIED_FIELD_REFERENCE_PATTERN = "(.*:)?\\$" + INPORT_REFERENCE_PATTERN + "\\.[_A-Za-z\\*]+[_A-Za-z0-9\\*]*";
	
	private Map<ObjectRepresentation, Map<MappingProperty, SortedSet<MappingError>>> errorsMap = new HashMap<ObjectRepresentation, Map<MappingProperty, SortedSet<MappingError>>>();

	private Stack<Integer> availablePorts = new Stack<Integer>();

	private final Map<Integer, DataRecordMetadata> inPorts;
	private ObjectElement globalPartition = null;
	private boolean errors = false;
	
	private int maxErrors = 50;			// How many full error messages to collect. If exceeded, only counter gets increased
	private int maxErrorsLimit = 100;	// If there are more errors, validation will be interrupted
	private int maxWarnings = 50;		// How many full warning messages to collect. If exceeded, only counter gets increased
	private int errorsCount;
	private int warningsCount;
	private boolean runIt = true;

	public MappingValidator(Map<Integer, DataRecordMetadata> inPorts) {
		this.inPorts = inPorts;
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

	public void setMapping(Mapping mapping) {
		super.setMapping(mapping);
		clear();
	}
	
	public boolean containsErrors() { 
		return errors;
	}

	public Map<ObjectRepresentation, Map<MappingProperty, SortedSet<MappingError>>> getErrorsMap() {
		return errorsMap;
	}

	@Override
	public void visit(ObjectAggregate element) throws Exception {
		if (!runIt) {
			return;
		}

		checkCloverNamespaceAvailable(element.getParent());
		
		MappingError error;
		
		String includeString = element.getProperty(MappingProperty.INCLUDE);
		String excludeString = element.getProperty(MappingProperty.EXCLUDE);
		String writeNullString;
		String omitNullString;
		
		Set<String> attributeNames;
		if (element.isElement()) {
			writeNullString = element.getProperty(MappingProperty.WRITE_NULL_ELEMENT);
			omitNullString = element.getProperty(MappingProperty.OMIT_NULL_ELEMENT);
			attributeNames = new HashSet<String>();
		} else {
			writeNullString = element.getParent().getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
			omitNullString = element.getParent().getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);
			attributeNames = collectAttributeNames(element.getParent());
		}
		
		if (includeString == null && excludeString == null) {
			error = new MappingError("Missing attribute " + MappingProperty.INCLUDE.getName(), Severity.ERROR);
			addProblem(element, MappingProperty.INCLUDE, error);
			
			error = new MappingError("Missing attribute " + MappingProperty.EXCLUDE.getName(), Severity.ERROR);
			addProblem(element, MappingProperty.EXCLUDE, error);
		}

		Set<DataFieldMetadataWrapper> availableFields = new HashSet<DataFieldMetadataWrapper>();
		if (includeString != null) {
			String[] include = includeString.split(Mapping.DELIMITER);
			for (String aggregateExpression : include) {
				if (!checkAggregateExpressionFormat(aggregateExpression, element, MappingProperty.INCLUDE)) {
					continue;
				}
				
				ParsedFieldExpression parsed = parseAggregateExpression(aggregateExpression);
				List<DataFieldMetadataWrapper> localAvailableFields = getFields(parsed, element, MappingProperty.INCLUDE);
				if (!availableFields.addAll(localAvailableFields)) {
					addNoEffectWarning(element, MappingProperty.INCLUDE, aggregateExpression);
				}
			}
		} else {
			for (Integer inPortIndex : availablePorts) {
				DataRecordMetadata dataRecordMetadata = inPorts.get(inPortIndex);
				DataFieldMetadata[] fields = dataRecordMetadata.getFields();
				for (int i = 0; i < fields.length; i++) {
					availableFields.add(new DataFieldMetadataWrapper(inPortIndex, i, fields[i], null));
				}
			}
		}
		
		if (excludeString != null) {
			String[] exclude = excludeString.split(Mapping.DELIMITER);
			for (String aggregateExpression : exclude) {
				if (!checkAggregateExpressionFormat(aggregateExpression, element, MappingProperty.EXCLUDE)) {
					continue;
				}
				
				ParsedFieldExpression parsed = parseAggregateExpression(aggregateExpression);
				List<DataFieldMetadataWrapper> localAvailableFields = getFields(parsed, element, MappingProperty.EXCLUDE);
				if (!availableFields.removeAll(localAvailableFields)) {
					addNoEffectWarning(element, MappingProperty.EXCLUDE, aggregateExpression);
				}
			}
		}
		
		if (!element.isElement()) {
			for (DataFieldMetadataWrapper wrapper : availableFields) {
				String name;
				if (wrapper.getNamespace() != null) {
					name = wrapper.getNamespace() + ":" + wrapper.getDataFieldMetadata().getName();
				} else {
					name = wrapper.getDataFieldMetadata().getName();
				}
				if (!attributeNames.add(name)) {
					error = new MappingError("Duplicate attribute name '" + name + "'", Severity.WARNING);
					addProblem(element, MappingProperty.INCLUDE, error);
				}
			}
		}
		
		availableFields.clear();
		if (writeNullString != null) {
			MappingProperty property = element.isElement() ? MappingProperty.WRITE_NULL_ELEMENT : MappingProperty.WRITE_NULL_ATTRIBUTE;
			ObjectRepresentation errorElement = element.isElement() ? element : element.getParent();
			String[] writeNull = writeNullString.split(Mapping.DELIMITER);
			for (String aggregateExpression : writeNull) {
				if (attributeNames.contains(aggregateExpression)) {
					continue;
				}
				if (!checkAggregateExpressionFormat(aggregateExpression, errorElement, property)) {
					continue;
				}
				
				ParsedFieldExpression parsed = parseAggregateExpression(aggregateExpression);
				List<DataFieldMetadataWrapper> localAvailableFields = getFields(parsed, errorElement, property);
				if (!availableFields.addAll(localAvailableFields)) {
					addNoEffectWarning(errorElement, property, aggregateExpression);
				}
			}
		} else if (omitNullString != null) {
			for (Integer inPortIndex : availablePorts) {
				DataRecordMetadata dataRecordMetadata = inPorts.get(inPortIndex);
				DataFieldMetadata[] fields = dataRecordMetadata.getFields();
				for (int i = 0; i < fields.length; i++) {
					availableFields.add(new DataFieldMetadataWrapper(inPortIndex, i, fields[i], null));
				}
			}
		}
		
		if (omitNullString != null) {
			MappingProperty property = element.isElement() ? MappingProperty.OMIT_NULL_ELEMENT : MappingProperty.OMIT_NULL_ATTRIBUTE;
			ObjectRepresentation errorElement = element.isElement() ? element : element.getParent();
			String[] omitNull = omitNullString.split(Mapping.DELIMITER);
			for (String aggregateExpression : omitNull) {
				if (attributeNames.contains(aggregateExpression)) {
					continue;
				}
				if (!checkAggregateExpressionFormat(aggregateExpression, errorElement, property)) {
					continue;
				}
				
				ParsedFieldExpression parsed = parseAggregateExpression(aggregateExpression);
				List<DataFieldMetadataWrapper> localAvailableFields = getFields(parsed, errorElement, property);
				if (!availableFields.removeAll(localAvailableFields)) {
					addNoEffectWarning(errorElement, property, aggregateExpression);
				}
			}
		}
	}
	
	/**
	 * Checks whether given aggregate expression is in valid format and possibly add error to corresponding element 
	 * 
	 * @param aggregateExpression expression to check
	 * @param errorElement element which's property is checked
	 * @param property property which is checked
	 * @return true if expression is valid, false otherwise
	 */
	private boolean checkAggregateExpressionFormat(String aggregateExpression, ObjectRepresentation errorElement, MappingProperty property) {
		if (!aggregateExpression.matches(QUALIFIED_FIELD_REFERENCE_PATTERN)) {
			addProblem(errorElement, property, new MappingError("Invalid expression '" + aggregateExpression + "'", Severity.ERROR));
			return false;
		}
		return true;
	}
	/**
	 * Adds warning about redundancy of given subexpression in a given property.
	 * 
	 * @param errorElement property owner
	 * @param property invalid property
	 * @param expression subexpression with no effect
	 */
	private void addNoEffectWarning(ObjectRepresentation errorElement, MappingProperty property, String expression) {
		addProblem(errorElement, property, new MappingError("Expression '" + expression + "' has no effect", Severity.WARNING));
	}

	@Override
	public void visit(ObjectAttribute element) throws Exception {
		if (!runIt) {
			return;
		}
		String name = element.getProperty(MappingProperty.NAME);
		if (name == null) {
			addProblem(element, MappingProperty.NAME, new MappingError("Empty name", Severity.ERROR));
		} else {
			int colonIndex = name.indexOf(':');
			if (colonIndex >= 0) {
				checkNamespacePrefixAvailable(element, name.substring(0, colonIndex), MappingProperty.NAME);
			}
		}
		validateValue(element);
	}

	@Override
	public void visit(ObjectElement element) throws Exception {
		if (!runIt || isInRecursion()) {
			return;
		}
		
		if (element.getParent() == null) {
			visitChildren(element);
			return;
		}
		
		if (element.isTemplate()) {
			checkCloverNamespaceAvailable(element);
			String templateName = element.getProperty(MappingProperty.TEMPLATE_NAME);
			if (templateName == null) {
				addProblem(element, MappingProperty.TEMPLATE_NAME, new MappingError("Unspecified template name", Severity.ERROR));
			}
			return;
		}
		
		String name = element.getProperty(MappingProperty.NAME);
		if (StringUtils.isEmpty(name)) {
			addProblem(element, MappingProperty.NAME, new MappingError("Name must not be empty", Severity.ERROR));
		} else {
			int colonIndex = name.indexOf(':');
			if (colonIndex >= 0) {
				checkNamespacePrefixAvailable(element, element, name.substring(0, colonIndex), MappingProperty.NAME);
			}
		}
		
		checkCorrectBooleanValue(element, MappingProperty.WRITE_NULL_ELEMENT);
		
		List<Integer> addedPorts = null;
		RecurringElementInfo recurringInfo = element.getRecurringInfo();
		if (recurringInfo != null) {
			String inPortString = recurringInfo.getProperty(MappingProperty.DATASCOPE);
			if (inPortString != null) {
				addedPorts = getPortIndexes(inPortString, inPorts);
				if (addedPorts.size() > 1) {
					addProblem(recurringInfo, MappingProperty.DATASCOPE, new MappingError("Ambiguous ports!", Severity.WARNING));
				}
				for (Integer inputPortIndex : addedPorts) {
					availablePorts.push(inputPortIndex);
				}
			}
		} else {
			String hideString = element.getProperty(MappingProperty.HIDE);
			if (hideString != null) {
				checkCorrectBooleanValue(element, MappingProperty.HIDE);
				if (Boolean.parseBoolean(hideString)) {
					addProblem(element, MappingProperty.HIDE, new MappingError("Only element with input port connected can be hidden", Severity.ERROR));
				}
			}
		}

		if (element.getParent().getParent() == null && recurringInfo != null) {
			addProblem(element, MappingProperty.UNKNOWN, new MappingError("Root element cannot be a loop element", Severity.ERROR));
			return;
		}
		if (element.getAttributeInfo() == null) {
			String writeNull = element.getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
			String omitNull = element.getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);
			
			Set<String> attributeNames = collectAttributeNames(element);
			if (writeNull != null || omitNull != null) {
				checkCloverNamespaceAvailable(element);
				
				if (writeNull != null) {
					for (String expression : writeNull.split(Mapping.DELIMITER)) {
						if (!attributeNames.contains(expression)) {
							addNoEffectWarning(element, MappingProperty.WRITE_NULL_ATTRIBUTE, expression);
						}
					}
				}
				if (omitNull != null) {
					for (String expression : omitNull.split(Mapping.DELIMITER)) {
						if (!attributeNames.contains(expression)) {
							addNoEffectWarning(element, MappingProperty.OMIT_NULL_ATTRIBUTE, expression);
						}
					}
				}
			}
		}
		
		String partitionString = element.getProperty(MappingProperty.PARTITION);
		if (partitionString != null) {
			checkCorrectBooleanValue(element, MappingProperty.PARTITION);
			boolean partition = Boolean.parseBoolean(partitionString);
			if (partition) {
				if (getRecurringParent(element.getParent()) != null) {
					addProblem(element, MappingProperty.PARTITION, new MappingError(
							"Partition element must be top level recurring element", Severity.ERROR));
				}
				if (globalPartition != null) {
					addProblem(element, MappingProperty.PARTITION, new MappingError(
							"There can be only one partition element defined", Severity.ERROR));
					addProblem(globalPartition, MappingProperty.PARTITION, new MappingError(
							"There can be only one partition element defined", Severity.ERROR));
				} else {
					globalPartition = element;
				}
			}
		}
		
		if (!runIt) {
			return;
		}
		visitChildren(element);

		if (addedPorts != null) {
			for (int i = 0; i < addedPorts.size(); i++) {
				availablePorts.pop();
			}
		}
	}
	
	/**
	 * Extract property from given element and check whether returned String is of boolean type (TRUE/FALSE or 
	 * additionally emptyString/null) otherwise add exception to user view window.
	 * @param element ObjectRepresentation
	 * @param property MappingProperty
	 */
	private void checkCorrectBooleanValue(ObjectRepresentation element, MappingProperty property) {
		String value = element.getProperty(property);
		if(StringUtils.isEmpty(value))
			return;
		if(!"TRUE".equalsIgnoreCase(value) && !"FALSE".equalsIgnoreCase(value))
			addProblem(element, MappingProperty.HIDE, new MappingError("Attribute accepts only boolean type values (true/false)", Severity.ERROR));
	}
	
	private Set<String> collectAttributeNames(ObjectElement element) {
		Set<String> attributeNames = new HashSet<String>();
		for (ObjectAttribute attribute : element.getAttributes()) {
			if (!attributeNames.add(attribute.getProperty(MappingProperty.NAME))) {
				addProblem(attribute, MappingProperty.NAME,
						new MappingError("Duplicate attribute name " + attribute.getProperty(MappingProperty.NAME), Severity.WARNING));
			}
		}
		return attributeNames;
	}
	
	@Override
	public void visit(ObjectNamespace element) {
		if (!runIt) {
			return;
		}
		if (element.getProperty(MappingProperty.VALUE) == null) {
			addProblem(element, MappingProperty.VALUE, new MappingError("URI not specified.", Severity.ERROR));
		}
		if (isNamespacePrefixAvailable(element.getParent(), element.getProperty(MappingProperty.NAME), element)) {
			MappingError error = new MappingError("Prefix '" + element.getProperty(MappingProperty.NAME) + "' is already declared in this scope", Severity.ERROR);
			addProblem(element, MappingProperty.NAME, error);
		}
	}
	
	

	@Override
	public void visit(ObjectComment element) throws Exception {
		if (!runIt) {
			return;
		}
		checkCorrectBooleanValue(element, MappingProperty.INCLUDE);
		validateValue(element);
	}

	@Override
	public void visit(ObjectValue element) {
		if (!runIt) {
			return;
		}
		validateValue(element);
	}
	
	private void validateValue(ObjectRepresentation element) {
		String value = element.getProperty(MappingProperty.VALUE);
		if (value == null) {
			addProblem(element, MappingProperty.VALUE, new MappingError("Empty value", Severity.WARNING));
			return;
		}
		
		List<ParsedFieldExpression> fields = parseValueExpression(value);
		for (ParsedFieldExpression parsedFieldExpression : fields) {
			Integer inPortIndex = getAvailableInputPort(parsedFieldExpression.getPort(), element, MappingProperty.VALUE);
			if (inPortIndex == null) {
				addProblem(element, MappingProperty.DATASCOPE, new MappingError("Input port '" + parsedFieldExpression.getPort() + "' is not connected!", Severity.ERROR));
			} else if (inPorts.get(inPortIndex).getField(parsedFieldExpression.getFields()) == null) {
				addProblem(element, MappingProperty.VALUE,
						new MappingError("Field '" + parsedFieldExpression.getFields() + "' is not available.", Severity.ERROR));
			}
		}
	}

	@Override
	public void visit(RecurringElementInfo element) throws Exception {
		if (!runIt) {
			return;
		}
		checkCloverNamespaceAvailable(element.getParent());
		
		String inPortString = element.getProperty(MappingProperty.DATASCOPE);
		Integer inPortIndex = null;
		if (inPortString == null) {
			addProblem(element, MappingProperty.DATASCOPE, new MappingError("Input port not specified!", Severity.ERROR));
			return;
		} else {
			inPortIndex = getAvailableInputPort(inPortString, element, MappingProperty.DATASCOPE);
			if (inPortIndex == null) {
				addProblem(element, MappingProperty.DATASCOPE, new MappingError("Input port '" + inPortString + "' is not connected!", Severity.ERROR));
				return;
			}
		}
		
		String keyString = element.getProperty(MappingProperty.KEY);
		String parentKeyString = element.getProperty(MappingProperty.PARENTKEY);
		
		if (parentKeyString != null && keyString == null) {
			addProblem(element, MappingProperty.KEY,
					new MappingError(MappingProperty.KEY.getName() + " attribute not specified!", Severity.ERROR));
		}
		if (parentKeyString == null && keyString != null) {
			addProblem(element, MappingProperty.PARENTKEY,
					new MappingError(MappingProperty.PARENTKEY.getName() + " attribute not specified!", Severity.ERROR));
		}
		
		if (keyString != null) {
			String[] keyList = keyString.split(Mapping.DELIMITER);
			checkAvailableData(element, MappingProperty.KEY, inPorts.get(inPortIndex), keyList);
			
			if (parentKeyString != null) {
				if (parentKeyString.split(Mapping.DELIMITER).length != keyList.length) {
					addProblem(element, MappingProperty.KEY, new MappingError(
							"Count of fields must match parent key field count", Severity.ERROR));
					addProblem(element, MappingProperty.PARENTKEY, new MappingError(
							"Count of fields must match key field count", Severity.ERROR));
				}
			}
			
		}
		if (parentKeyString != null) {
			inPortString = null;
			ObjectElement parent = getRecurringParent(element.getParent());
			if (parent != null) {
				inPortString = parent.getRecurringInfo().getProperty(MappingProperty.DATASCOPE);
			}
			if (inPortString == null) {
				addProblem(element, MappingProperty.PARENTKEY, new MappingError("No data for parent key fields!", Severity.ERROR));
			} else {
				inPortIndex = getAvailableInputPort(inPortString, element, MappingProperty.PARENTKEY);
				if (inPortIndex == null) {
					addProblem(element, MappingProperty.PARENTKEY, new MappingError("No data for parent key fields!", Severity.ERROR));
				} else {
					checkAvailableData(element, MappingProperty.PARENTKEY, inPorts.get(inPortIndex),
							parentKeyString.split(Mapping.DELIMITER));
				}
			}
		}	
	}

	@Override
	public void visit(ObjectTemplateEntry objectTemplateEntry) throws Exception {
		if (!runIt) {
			return;
		}
		checkCloverNamespaceAvailable(objectTemplateEntry.getParent());
		
		String templateName = objectTemplateEntry.getProperty(MappingProperty.TEMPLATE_NAME); 
		if (templateName == null || !mapping.getTemplates().containsKey(templateName)) {
			addProblem(objectTemplateEntry, MappingProperty.NAME, new MappingError("Unknown template", Severity.ERROR));
			return;
		}
		super.visit(objectTemplateEntry);
	}

	private List<DataFieldMetadataWrapper> getFields(ParsedFieldExpression fieldExpression, ObjectRepresentation element, MappingProperty property) {
		List<DataFieldMetadataWrapper> availableFields = new ArrayList<DataFieldMetadataWrapper>();

		Integer inPortIndex = getAvailableInputPort(fieldExpression.getPort(), element, property);
		if (inPortIndex != null) {
			String fieldsString = fieldExpression.getFields();
			DataFieldMetadata[] fields = inPorts.get(inPortIndex).getFields();
			for (int i = 0; i < fields.length; i++) {
				DataFieldMetadata field = fields[i]; 
				if (field.getName().matches(fieldsString)) {
					availableFields.add(new DataFieldMetadataWrapper(inPortIndex, i, field, fieldExpression.getNamespace()));
				}
			}
		} else {
			addProblem(element, property, new MappingError("Port '" + fieldExpression.getPort() + "' is not available", Severity.ERROR));
		}
		if (fieldExpression.getNamespace() != null) {
			checkNamespacePrefixAvailable(element, fieldExpression.getNamespace(), property);
		}
		
		return availableFields;
	}
	
	private List<Integer> getPortIndexes(String inPortString, Map<Integer, DataRecordMetadata> availablePorts) {
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
				if (entry.getValue().getName().equals(inPortString)) {
					toReturn.add(entry.getKey());
				}
			}
		}
		return toReturn;
	}

	private Integer getAvailableInputPort(String key, ObjectRepresentation element, MappingProperty keyword) {
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

	private void checkAvailableData(ObjectRepresentation element, MappingProperty property, DataRecordMetadata metadata,
			String[] fieldNames) {
		for (String fieldName : fieldNames) {
			if (metadata.getField(fieldName) == null) {
				addProblem(element, property, new MappingError("Unknown field '" + fieldName + "'", Severity.ERROR));
			}
		}
	}

	private void addProblem(ObjectRepresentation element, MappingProperty property, MappingError error) {
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
	
	private void checkCloverNamespaceAvailable(ObjectElement element) {
		if (!isCloverNamespaceAvailable(element)) {
			addProblem(element, MappingProperty.UNKNOWN, new MappingError("Clover namespace is not available!", Severity.ERROR));
		}
	}
	
	private void checkNamespacePrefixAvailable(ObjectRepresentation element, String prefix, MappingProperty property) {
		checkNamespacePrefixAvailable(element, element.getParent(), prefix, property);
	}
	
	private void checkNamespacePrefixAvailable(ObjectRepresentation source, ObjectElement parent, String prefix, MappingProperty property) {
		if (!isNamespacePrefixAvailable(parent, prefix, null)) {
			addProblem(source, property, new MappingError("Namespace '" + prefix + "' is not available!", Severity.ERROR));
		}
	}
	
	
	private boolean isNamespacePrefixAvailable(ObjectElement element, String prefix, ObjectNamespace exclude) {
		for (ObjectNamespace namespace : element.getNamespaces()) {
			if (exclude == namespace) {
				continue;
			}
			String otherPrefix = namespace.getProperty(MappingProperty.NAME);
			if (otherPrefix == prefix) {
				return true;
			} else if (prefix != null && prefix.equals(otherPrefix)) {
				return true;
			}
		}
		if (element.getParent() != null) {
			return isNamespacePrefixAvailable(element.getParent(), prefix, exclude);
		}
		return false;
	}
	
	private boolean isCloverNamespaceAvailable(ObjectElement element) {
		for (ObjectNamespace namespace : element.getNamespaces()) {
			if (Mapping.MAPPING_KEYWORDS_NAMESPACEURI.equals(namespace.getProperty(MappingProperty.VALUE))) {
				return true;
			}
		}
		if (element.getParent() != null) {
			return isCloverNamespaceAvailable(element.getParent());
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
	
	private static final class SeverityComparator implements Comparator<MappingError> {
		
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
	
}

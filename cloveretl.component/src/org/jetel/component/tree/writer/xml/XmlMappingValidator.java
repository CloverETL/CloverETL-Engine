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
package org.jetel.component.tree.writer.xml;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.saxon.om.Name11Checker;

import org.jetel.component.tree.writer.model.design.AbstractNode;
import org.jetel.component.tree.writer.model.design.Attribute;
import org.jetel.component.tree.writer.model.design.Comment;
import org.jetel.component.tree.writer.model.design.ContainerNode;
import org.jetel.component.tree.writer.model.design.MappingProperty;
import org.jetel.component.tree.writer.model.design.Namespace;
import org.jetel.component.tree.writer.model.design.ObjectNode;
import org.jetel.component.tree.writer.model.design.Relation;
import org.jetel.component.tree.writer.model.design.TemplateEntry;
import org.jetel.component.tree.writer.model.design.TreeWriterMapping;
import org.jetel.component.tree.writer.model.design.TreeWriterMappingUtil;
import org.jetel.component.tree.writer.model.design.WildcardAttribute;
import org.jetel.component.tree.writer.model.design.WildcardNode;
import org.jetel.component.tree.writer.util.AbstractMappingValidator;
import org.jetel.component.tree.writer.util.DataFieldMetadataWrapper;
import org.jetel.component.tree.writer.util.MappingError;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;



/**
 * Visitor which validates xml mapping.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 15 Dec 2010
 */
public class XmlMappingValidator extends AbstractMappingValidator {

	private final static String INPORT_REFERENCE_PATTERN = "(" + StringUtils.OBJECT_NAME_PATTERN + "|[0-9]+)";
	public final static String FIELD_REFERENCE_PATTERN = "\\$" + INPORT_REFERENCE_PATTERN + "\\.[_A-Za-z\\*]+[_A-Za-z0-9\\*]*";
	public final static String QUALIFIED_FIELD_REFERENCE_PATTERN = "(.*:)?" + FIELD_REFERENCE_PATTERN;

	private boolean oneRecordPerFile = false;

	public XmlMappingValidator(Map<Integer, DataRecordMetadata> inPorts, boolean oneRecordPerFile) {
		super(inPorts);
		this.oneRecordPerFile = oneRecordPerFile;
	}

	@Override
	protected void validateWildCardNode(WildcardNode element) {
		String includeString = element.getProperty(MappingProperty.INCLUDE);
		String excludeString = element.getProperty(MappingProperty.EXCLUDE);
		wildcardIncludeExcludeCheck(includeString, excludeString, element);

		gatherAndValidateAvailableFields(includeString, excludeString, element);

		String writeNullString = element.getProperty(MappingProperty.WRITE_NULL_ELEMENT);
		String omitNullString = element.getProperty(MappingProperty.OMIT_NULL_ELEMENT);
		reportWildcardNullProblems(writeNullString, omitNullString, element, true, new HashSet<String>());
	}

	@Override
	protected void validateWildCardAttribute(WildcardAttribute element) {
		String includeString = element.getProperty(MappingProperty.INCLUDE);
		String excludeString = element.getProperty(MappingProperty.EXCLUDE);
		wildcardIncludeExcludeCheck(includeString, excludeString, element);

		Set<DataFieldMetadataWrapper> availableFields = gatherAndValidateAvailableFields(includeString, excludeString, element);

		Set<String> attributeNames = collectAttributeNames(element.getParent());
		for (DataFieldMetadataWrapper wrapper : availableFields) {
			String name;
			if (wrapper.getNamespace() != null) {
				name = wrapper.getNamespace() + ":" + wrapper.getDataFieldMetadata().getName();
			} else {
				name = wrapper.getDataFieldMetadata().getName();
			}
			if (!attributeNames.add(name)) {
				MappingError error = new MappingError("Duplicate attribute name '" + name + "'", Severity.WARNING);
				addProblem(element, MappingProperty.INCLUDE, error);
			}
		}

		String writeNullString = element.getParent().getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
		String omitNullString = element.getParent().getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);
		reportWildcardNullProblems(writeNullString, omitNullString, element, false, attributeNames);
	}

	private void reportWildcardNullProblems(String writeNullString, String omitNullString, AbstractNode element,
			boolean isElement, Set<String> attributeNames) {
		Set<DataFieldMetadataWrapper> availableFields = new HashSet<DataFieldMetadataWrapper>();

		if (writeNullString != null) {
			MappingProperty property = isElement ? MappingProperty.WRITE_NULL_ELEMENT : MappingProperty.WRITE_NULL_ATTRIBUTE;
			AbstractNode errorElement = isElement ? element : element.getParent();
			String[] writeNull = writeNullString.split(TreeWriterMapping.DELIMITER);
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
				if (dataRecordMetadata != null) {
					DataFieldMetadata[] fields = dataRecordMetadata.getFields();
					for (int i = 0; i < fields.length; i++) {
						availableFields.add(new DataFieldMetadataWrapper(inPortIndex, i, fields[i], null));
					}
				} else {
					addProblem(element, MappingProperty.OMIT_NULL_ELEMENT, new MappingError("Metadata of port '" + inPortIndex + "' not available", Severity.ERROR));
				}
			}
		}

		if (omitNullString != null) {
			MappingProperty property = isElement ? MappingProperty.OMIT_NULL_ELEMENT : MappingProperty.OMIT_NULL_ATTRIBUTE;
			AbstractNode errorElement = isElement ? element : element.getParent();
			String[] omitNull = omitNullString.split(TreeWriterMapping.DELIMITER);
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

	private void wildcardIncludeExcludeCheck(String includeString, String excludeString, AbstractNode wildcard) {
		if (includeString == null && excludeString == null) {
			MappingError error = new MappingError("Missing attribute " + MappingProperty.INCLUDE.getName(), Severity.ERROR);
			addProblem(wildcard, MappingProperty.INCLUDE, error);

			error = new MappingError("Missing attribute " + MappingProperty.EXCLUDE.getName(), Severity.ERROR);
			addProblem(wildcard, MappingProperty.EXCLUDE, error);
		}
	}

	protected Set<DataFieldMetadataWrapper> gatherAndValidateAvailableFields(String includeString, String excludeString,
			AbstractNode wildcard) {
		Set<DataFieldMetadataWrapper> availableFields = new HashSet<DataFieldMetadataWrapper>();
		if (includeString != null) {
			String[] include = includeString.split(TreeWriterMapping.DELIMITER);
			for (String aggregateExpression : include) {
				if (!checkAggregateExpressionFormat(aggregateExpression, wildcard, MappingProperty.INCLUDE)) {
					continue;
				}

				ParsedFieldExpression parsed = parseAggregateExpression(aggregateExpression);
				List<DataFieldMetadataWrapper> localAvailableFields = getFields(parsed, wildcard, MappingProperty.INCLUDE);
				if (!availableFields.addAll(localAvailableFields)) {
					addNoEffectWarning(wildcard, MappingProperty.INCLUDE, aggregateExpression);
				}
			}
		} else {
			for (Integer inPortIndex : availablePorts) {
				DataRecordMetadata dataRecordMetadata = inPorts.get(inPortIndex);
				if (dataRecordMetadata != null) {
					DataFieldMetadata[] fields = dataRecordMetadata.getFields();
					for (int i = 0; i < fields.length; i++) {
						availableFields.add(new DataFieldMetadataWrapper(inPortIndex, i, fields[i], null));
					}
				} else {
					addProblem(wildcard, MappingProperty.INCLUDE, new MappingError("Metadata of port '" + inPortIndex + "' not available", Severity.ERROR));
				}
			}
		}

		if (excludeString != null) {
			String[] exclude = excludeString.split(TreeWriterMapping.DELIMITER);
			for (String aggregateExpression : exclude) {
				if (!checkAggregateExpressionFormat(aggregateExpression, wildcard, MappingProperty.EXCLUDE)) {
					continue;
				}

				ParsedFieldExpression parsed = parseAggregateExpression(aggregateExpression);
				List<DataFieldMetadataWrapper> localAvailableFields = getFields(parsed, wildcard, MappingProperty.EXCLUDE);
				if (!availableFields.removeAll(localAvailableFields)) {
					addNoEffectWarning(wildcard, MappingProperty.EXCLUDE, aggregateExpression);
				}
			}
		}

		return availableFields;
	}

	/**
	 * Checks whether given aggregate expression is in valid format and possibly add error to corresponding element
	 * 
	 * @param aggregateExpression
	 *            expression to check
	 * @param errorElement
	 *            element which's property is checked
	 * @param property
	 *            property which is checked
	 * @return true if expression is valid, false otherwise
	 */
	private boolean checkAggregateExpressionFormat(String aggregateExpression, AbstractNode errorElement,
			MappingProperty property) {
		if (!aggregateExpression.matches(QUALIFIED_FIELD_REFERENCE_PATTERN)) {
			addProblem(errorElement, property, new MappingError("Invalid expression '" + aggregateExpression + "'", Severity.ERROR));
			return false;
		}
		return true;
	}

	/**
	 * Adds warning about redundancy of given subexpression in a given property.
	 * 
	 * @param errorElement
	 *            property owner
	 * @param property
	 *            invalid property
	 * @param expression
	 *            subexpression with no effect
	 */
	private void addNoEffectWarning(AbstractNode errorElement, MappingProperty property, String expression) {
		addProblem(errorElement, property, new MappingError("Expression '" + expression + "' has no effect", Severity.WARNING));
	}

	@Override
	protected void validateAttribute(Attribute element) {
		String name = element.getProperty(MappingProperty.NAME);
		if (validateName(element, name)) {
			int colonIndex = name.indexOf(':');
			if (colonIndex >= 0) {
				checkNamespacePrefixAvailable(element, name.substring(0, colonIndex), MappingProperty.NAME);
			}
		}
		validateValue(element);
	}

	@Override
	protected void validateElement(ObjectNode element) {
		String name = element.getProperty(MappingProperty.NAME);
		if (validateName(element, name)) {
			int colonIndex = name.indexOf(':');
			if (colonIndex >= 0) {
				checkNamespacePrefixAvailable(element, element, name.substring(0, colonIndex), MappingProperty.NAME);
			}
		}
		Relation recurringInfo = element.getRelation();
		if (element.getParent().getParent() == null && recurringInfo != null && !oneRecordPerFile) {
			addProblem(element, MappingProperty.UNKNOWN, new MappingError("Port binding to a root element may produce invalid XML file. Set 'Records per file' or 'Max number of records' component attributes to '1'.", Severity.WARNING));
		}
		if (element.getWildcardAttribute() == null) {
			String writeNull = element.getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
			String omitNull = element.getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);

			Set<String> attributeNames = collectAttributeNames(element);
			if (writeNull != null || omitNull != null) {
				checkCloverNamespaceAvailable(element);

				if (writeNull != null) {
					for (String expression : writeNull.split(TreeWriterMapping.DELIMITER)) {
						if (!attributeNames.contains(expression)) {
							addNoEffectWarning(element, MappingProperty.WRITE_NULL_ATTRIBUTE, expression);
						}
					}
				}
				if (omitNull != null) {
					for (String expression : omitNull.split(TreeWriterMapping.DELIMITER)) {
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
					addProblem(element, MappingProperty.PARTITION, new MappingError("Partition element must be top level recurring element", Severity.ERROR));
				}
				if (globalPartition != null) {
					addProblem(element, MappingProperty.PARTITION, new MappingError("There can be only one partition element defined", Severity.ERROR));
					addProblem(globalPartition, MappingProperty.PARTITION, new MappingError("There can be only one partition element defined", Severity.ERROR));
				} else {
					globalPartition = element;
				}
			}
		}

		if (recurringInfo == null) {
			String hideString = element.getProperty(MappingProperty.HIDE);
			if (hideString != null) {
				if (Boolean.parseBoolean(hideString)) {
					addProblem(element, MappingProperty.HIDE, new MappingError("Only element with input port connected can be hidden", Severity.ERROR));
				}
			}
		}

	}

	private Set<String> collectAttributeNames(ContainerNode container) {
		Set<String> attributeNames = new HashSet<String>();

		if (container instanceof ObjectNode) {
			ObjectNode attributeSupportingContainer = (ObjectNode) container;
			for (Attribute attribute : attributeSupportingContainer.getAttributes()) {
				if (!attributeNames.add(attribute.getProperty(MappingProperty.NAME))) {
					addProblem(attribute, MappingProperty.NAME, new MappingError("Duplicate attribute name " + attribute.getProperty(MappingProperty.NAME), Severity.WARNING));
				}
			}
		}

		return attributeNames;
	}

	@Override
	protected void validateNamespace(Namespace element) {
		if (element.getProperty(MappingProperty.VALUE) == null) {
			addProblem(element, MappingProperty.VALUE, new MappingError("URI not specified.", Severity.ERROR));
		}
		if (TreeWriterMappingUtil.isNamespacePrefixAvailable(element.getParent(), element.getProperty(MappingProperty.NAME), element)) {
			MappingError error = new MappingError("Prefix '" + element.getProperty(MappingProperty.NAME) + "' is already declared in this scope", Severity.ERROR);
			addProblem(element, MappingProperty.NAME, error);
		}
	}

	@Override
	protected void validateComment(Comment comment) {
		checkCorrectBooleanValue(comment, MappingProperty.WRITE);
		validateValue(comment);
	}

	private boolean validateName(AbstractNode element, String name) {
		if (StringUtils.isEmpty(name)) {
			addProblem(element, MappingProperty.NAME, new MappingError("Name must not be empty", Severity.ERROR));
		} else {
			if (Name11Checker.getInstance().isQName(name)) {
				return true;
			} else if (Name11Checker.getInstance().isQName(name.replaceAll(FIELD_REFERENCE_PATTERN, "x"))) {
				return true;
			} else {
				addProblem(element, MappingProperty.NAME, new MappingError("Invalid name " + name, Severity.ERROR));
			}
		}
		return false;
	}

	@Override
	protected void validateRelation(Relation element) {
		// No format specific validation
	}

	@Override
	protected void validateTemplateEntry(TemplateEntry objectTemplateEntry) {
		// No format specific validation
	}

	private void checkNamespacePrefixAvailable(AbstractNode element, String prefix, MappingProperty property) {
		checkNamespacePrefixAvailable(element, element.getParent(), prefix, property);
	}

	private void checkNamespacePrefixAvailable(AbstractNode source, ContainerNode parent, String prefix,
			MappingProperty property) {
		if (!prefix.matches(FIELD_REFERENCE_PATTERN) && !TreeWriterMappingUtil.isNamespacePrefixAvailable(parent, prefix)) {
			addProblem(source, property, new MappingError("Namespace '" + prefix + "' is not available!", Severity.ERROR));
		}
	}

	protected List<DataFieldMetadataWrapper> getFields(ParsedFieldExpression fieldExpression, AbstractNode element,
			MappingProperty property) {
		List<DataFieldMetadataWrapper> availableFields = new ArrayList<DataFieldMetadataWrapper>();

		Integer inPortIndex = getAvailableInputPort(fieldExpression.getPort(), element, property);
		if (inPortIndex != null) {
			DataRecordMetadata recordMetadata = inPorts.get(inPortIndex);
			if (recordMetadata != null) {
				String fieldsString = fieldExpression.getFields();
				DataFieldMetadata[] fields = recordMetadata.getFields();
				for (int i = 0; i < fields.length; i++) {
					DataFieldMetadata field = fields[i];
					if (field.getName().matches(fieldsString)) {
						availableFields.add(new DataFieldMetadataWrapper(inPortIndex, i, field, fieldExpression.getNamespace()));
					}
				}
			} else {
				addProblem(element, property, new MappingError("Metadata of port '" + fieldExpression.getPort() + "' are not available", Severity.ERROR));
			}
		} else {
			addProblem(element, property, new MappingError("Port '" + fieldExpression.getPort() + "' is not available", Severity.ERROR));
		}
		if (fieldExpression.getNamespace() != null) {
			checkNamespacePrefixAvailable(element, fieldExpression.getNamespace(), property);
		}

		return availableFields;
	}
}

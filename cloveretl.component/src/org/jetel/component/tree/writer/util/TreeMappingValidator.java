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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.saxon.om.Name11Checker;

import org.jetel.component.tree.writer.model.design.AbstractNode;
import org.jetel.component.tree.writer.model.design.ContainerNode;
import org.jetel.component.tree.writer.model.design.MappingProperty;
import org.jetel.component.tree.writer.model.design.Namespace;
import org.jetel.component.tree.writer.model.design.Relation;
import org.jetel.component.tree.writer.model.design.TemplateEntry;
import org.jetel.component.tree.writer.model.design.TreeWriterMapping;
import org.jetel.component.tree.writer.model.design.TreeWriterMappingUtil;
import org.jetel.component.tree.writer.model.design.WildcardNode;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * Visitor which validates mapping for tree structured mapping.
 * 
 * @author hajdam (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 22 May 2015
 */
public abstract class TreeMappingValidator extends AbstractMappingValidator {

	private final static String INPORT_REFERENCE_PATTERN = "(" + StringUtils.OBJECT_NAME_PATTERN + "|[0-9]+)"; //$NON-NLS-1$ //$NON-NLS-2$
	public final static String FIELD_REFERENCE_PATTERN = "\\$" + INPORT_REFERENCE_PATTERN + "\\.[_A-Za-z\\*]+[_A-Za-z0-9\\*]*"; //$NON-NLS-1$ //$NON-NLS-2$
	public final static String FIELD_REFERENCE_PATTERN_REPLACEMENT = "x"; //$NON-NLS-1$
	public final static String QUALIFIED_FIELD_REFERENCE_PATTERN = "(.*:)?" + FIELD_REFERENCE_PATTERN; //$NON-NLS-1$

	private boolean oneRecordPerFile = false;

	public TreeMappingValidator(Map<Integer, DataRecordMetadata> inPorts, boolean oneRecordPerFile) {
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

	protected void reportWildcardNullProblems(String writeNullString, String omitNullString, AbstractNode element,
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
					addProblem(element, MappingProperty.OMIT_NULL_ELEMENT, new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.metadataOfPortNotAvailableError"), inPortIndex), Severity.ERROR)); //$NON-NLS-1$
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

	protected void wildcardIncludeExcludeCheck(String includeString, String excludeString, AbstractNode wildcard) {
		wildcardIncludeExcludeCheck(includeString, excludeString, wildcard, Severity.ERROR);
	}
	
	protected void wildcardIncludeExcludeCheck(String includeString, String excludeString, AbstractNode wildcard, Severity severity) {
		if (includeString == null && excludeString == null) {
			MappingError error = new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.missingAttributeError"), MappingProperty.INCLUDE.getName()), severity); //$NON-NLS-1$
			addProblem(wildcard, MappingProperty.INCLUDE, error);

			error = new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.missingAttributeError"), MappingProperty.EXCLUDE.getName()), severity); //$NON-NLS-1$
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
					addProblem(wildcard, MappingProperty.INCLUDE, new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.metadataOfPortNotAvailableError"), inPortIndex), Severity.ERROR)); //$NON-NLS-1$
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
	protected boolean checkAggregateExpressionFormat(String aggregateExpression, AbstractNode errorElement,
			MappingProperty property) {
		return checkAggregateExpressionFormat(aggregateExpression, errorElement, property, Severity.ERROR);
	}

	protected boolean checkAggregateExpressionFormat(String aggregateExpression, AbstractNode errorElement,
			MappingProperty property, Severity severity) {
		if (!aggregateExpression.matches(QUALIFIED_FIELD_REFERENCE_PATTERN)) {
			addProblem(errorElement, property, new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.invalidExpressionError"), aggregateExpression), severity)); //$NON-NLS-1$
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
	protected void addNoEffectWarning(AbstractNode errorElement, MappingProperty property, String expression) {
		addProblem(errorElement, property, new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.noEffectExpressionError"), expression), Severity.WARNING)); //$NON-NLS-1$
	}

	@Override
	protected void validateNamespace(Namespace element) {
		if (element.getProperty(MappingProperty.VALUE) == null) {
			addProblem(element, MappingProperty.VALUE, new MappingError(ValidatorMessages.getString("TreeMappingValidator.uriNotSpecifiedError"), Severity.ERROR)); //$NON-NLS-1$
		}
		if (TreeWriterMappingUtil.isNamespacePrefixAvailable(element.getParent(), element.getProperty(MappingProperty.NAME), element)) {
			MappingError error = new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.prefixAlreadyDeclaredError"), element.getProperty(MappingProperty.NAME)), Severity.ERROR); //$NON-NLS-1$
			addProblem(element, MappingProperty.NAME, error);
		}
	}

	protected boolean validateName(AbstractNode element, String name) {
		if (StringUtils.isEmpty(name)) {
			addProblem(element, MappingProperty.NAME, new MappingError(ValidatorMessages.getString("TreeMappingValidator.nameEmptyError"), Severity.ERROR)); //$NON-NLS-1$
		} else {
			if (Name11Checker.getInstance().isQName(name)) {
				return true;
			} else if (Name11Checker.getInstance().isQName(name.replaceAll(FIELD_REFERENCE_PATTERN, FIELD_REFERENCE_PATTERN_REPLACEMENT))) {
				validateFieldValue(element, name, MappingProperty.NAME);
				return true;
			} else {
				return validateBasicName(element, name);
			}
		}
		return false;
	}

	/**
	 * Validates basic name which is not QName.
	 * Basic validation returns problem, because such name is not valid tag name.
	 * 
	 * @param element
	 * @param name
	 * @return
	 */
	protected boolean validateBasicName(AbstractNode element, String name) {
		addProblem(element, MappingProperty.NAME, new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.invalidNameError"), name), Severity.ERROR)); //$NON-NLS-1$
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

	protected void checkNamespacePrefixAvailable(AbstractNode element, String prefix, MappingProperty property) {
		checkNamespacePrefixAvailable(element, element.getParent(), prefix, property);
	}

	protected void checkNamespacePrefixAvailable(AbstractNode source, ContainerNode parent, String prefix,
			MappingProperty property) {
		checkNamespacePrefixAvailable(source, parent, prefix, property, Severity.ERROR);
	}

	protected void checkNamespacePrefixAvailable(AbstractNode source, ContainerNode parent, String prefix,
			MappingProperty property, Severity severity) {
		if (!prefix.matches(FIELD_REFERENCE_PATTERN) && !TreeWriterMappingUtil.isNamespacePrefixAvailable(parent, prefix)) {
			addProblem(source, property, new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.namespaceNotAvailableError"), prefix), severity)); //$NON-NLS-1$
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
				addProblem(element, property, new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.metadataOfPortNotAvailableError"), fieldExpression.getPort()), Severity.ERROR)); //$NON-NLS-1$
			}
		} else {
			addProblem(element, property, new MappingError(MessageFormat.format(ValidatorMessages.getString("TreeMappingValidator.portNotAvailableError"), fieldExpression.getPort()), Severity.ERROR)); //$NON-NLS-1$
		}
		if (fieldExpression.getNamespace() != null) {
			checkNamespacePrefixAvailable(element, fieldExpression.getNamespace(), property);
		}

		return availableFields;
	}

	public boolean isOneRecordPerFile() {
		return oneRecordPerFile;
	}
}

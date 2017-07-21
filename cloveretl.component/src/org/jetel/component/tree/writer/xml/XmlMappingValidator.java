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

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jetel.component.tree.writer.model.design.Attribute;
import org.jetel.component.tree.writer.model.design.CDataSection;
import org.jetel.component.tree.writer.model.design.Comment;
import org.jetel.component.tree.writer.model.design.ContainerNode;
import org.jetel.component.tree.writer.model.design.MappingProperty;
import org.jetel.component.tree.writer.model.design.ObjectNode;
import org.jetel.component.tree.writer.model.design.Relation;
import org.jetel.component.tree.writer.model.design.TreeWriterMapping;
import org.jetel.component.tree.writer.model.design.WildcardAttribute;
import org.jetel.component.tree.writer.util.DataFieldMetadataWrapper;
import org.jetel.component.tree.writer.util.MappingError;
import org.jetel.component.tree.writer.util.TreeMappingValidator;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Visitor which validates xml mapping.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 15 Dec 2010
 */
public class XmlMappingValidator extends TreeMappingValidator {

	public XmlMappingValidator(Map<Integer, DataRecordMetadata> inPorts, boolean oneRecordPerFile) {
		super(inPorts, oneRecordPerFile);
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
				name = wrapper.getNamespace() + ":" + wrapper.getDataFieldMetadata().getName(); //$NON-NLS-1$
			} else {
				name = wrapper.getDataFieldMetadata().getName();
			}
			if (!attributeNames.add(name)) {
				MappingError error = new MappingError(MessageFormat.format(ValidatorMessages.getString("XmlMappingValidator.duplicateAttributeNameWarning"), name), Severity.WARNING); //$NON-NLS-1$
				addProblem(element, MappingProperty.INCLUDE, error);
			}
		}

		String writeNullString = element.getParent().getProperty(MappingProperty.WRITE_NULL_ATTRIBUTE);
		String omitNullString = element.getParent().getProperty(MappingProperty.OMIT_NULL_ATTRIBUTE);
		reportWildcardNullProblems(writeNullString, omitNullString, element, false, attributeNames);
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
		if (element.getParent().getParent() == null && recurringInfo != null && !isOneRecordPerFile()) {
			addProblem(element, MappingProperty.UNKNOWN, new MappingError(ValidatorMessages.getString("XmlMappingValidator.portBindingToRootWarning"), Severity.WARNING)); //$NON-NLS-1$
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
					addProblem(element, MappingProperty.PARTITION, new MappingError(ValidatorMessages.getString("XmlMappingValidator.partitionElementMustBeTopError"), Severity.ERROR)); //$NON-NLS-1$
				}
				if (globalPartition != null) {
					addProblem(element, MappingProperty.PARTITION, new MappingError(ValidatorMessages.getString("XmlMappingValidator.onlyOnePartitionAllowedError"), Severity.ERROR)); //$NON-NLS-1$
					addProblem(globalPartition, MappingProperty.PARTITION, new MappingError(ValidatorMessages.getString("XmlMappingValidator.onlyOnePartitionAllowedError"), Severity.ERROR)); //$NON-NLS-1$
				} else {
					globalPartition = element;
				}
			}
		}

		if (recurringInfo == null) {
			String hideString = element.getProperty(MappingProperty.HIDE);
			if (hideString != null) {
				if (Boolean.parseBoolean(hideString)) {
					addProblem(element, MappingProperty.HIDE, new MappingError(ValidatorMessages.getString("XmlMappingValidator.onlyElementWithInputPortCanBeHiddenError"), Severity.ERROR)); //$NON-NLS-1$
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
					addProblem(attribute, MappingProperty.NAME, new MappingError(MessageFormat.format(ValidatorMessages.getString("XmlMappingValidator.duplicateAttributeNameWarning"), attribute.getProperty(MappingProperty.NAME)), Severity.WARNING)); //$NON-NLS-1$
				}
			}
		}

		return attributeNames;
	}

	@Override
	protected void validateComment(Comment comment) {
		checkCorrectBooleanValue(comment, MappingProperty.WRITE);
		validateValue(comment);
	}
	
	@Override
	protected void validateCDataSection(CDataSection section) {
		validateValue(section);
	}
}

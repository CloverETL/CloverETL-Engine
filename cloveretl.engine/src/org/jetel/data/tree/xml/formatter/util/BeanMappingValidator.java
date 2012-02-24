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
package org.jetel.data.tree.xml.formatter.util;

import java.util.Map;

import org.jetel.data.tree.bean.schema.generator.SchemaGenerator;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.formatter.designmodel.AbstractNode;
import org.jetel.data.tree.formatter.designmodel.Attribute;
import org.jetel.data.tree.formatter.designmodel.Comment;
import org.jetel.data.tree.formatter.designmodel.ObjectNode;
import org.jetel.data.tree.formatter.designmodel.MappingProperty;
import org.jetel.data.tree.formatter.designmodel.Namespace;
import org.jetel.data.tree.formatter.designmodel.Relation;
import org.jetel.data.tree.formatter.designmodel.TemplateEntry;
import org.jetel.data.tree.formatter.designmodel.WildcardAttribute;
import org.jetel.data.tree.formatter.designmodel.WildcardNode;
import org.jetel.data.tree.formatter.util.MappingError;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.11.2011
 */
public class BeanMappingValidator extends AbstractMappingValidator {

	private SchemaObject schema;
	
	/**
	 * @param inputPorts
	 */
	public BeanMappingValidator(Map<Integer, DataRecordMetadata> inputPorts, SchemaObject schema) {
		super(inputPorts);
		this.schema = schema;
	}

	@Override
	protected void validateAttribute(Attribute attribute) {
	}

	@Override
	protected void validateComment(Comment comment) {
		addProblem(comment, MappingProperty.UNKNOWN, new MappingError("Bean mapping cannot contain comment.", Severity.ERROR));
	}

	@Override
	protected void validateRelation(Relation element) {
	}

	@Override
	protected void validateElement(ObjectNode element) {
		
		if (schema != null) {
			String path = element.getHierarchicalName();
			SchemaObject correspondingObject = SchemaGenerator.findSchemaObject(path, schema);
			if (correspondingObject == null) {
				addProblem(element, MappingProperty.UNKNOWN, new MappingError("Referenced object is not present in bean schema.", Severity.ERROR));
			}
		}
	}

	@Override
	protected void validateNamespace(Namespace element) {
	}

	@Override
	protected void validateTemplateEntry(TemplateEntry element) {
	}
	
	@Override
	protected void validateValue(AbstractNode element) {
		super.validateValue(element);
	}

	@Override
	protected void validateWildCardNode(WildcardNode element) {
		addProblem(element, MappingProperty.UNKNOWN, new MappingError("Bean mapping cannot contain wildcards.", Severity.ERROR));
	}

	@Override
	protected void validateWildCardAttribute(WildcardAttribute element) {
		addProblem(element, MappingProperty.UNKNOWN, new MappingError("Bean mapping cannot contain wildcards.", Severity.ERROR));
	}
}

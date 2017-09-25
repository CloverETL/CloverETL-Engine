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
package org.jetel.component.tree.writer.model.design;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

import javax.xml.stream.XMLStreamException;

import org.jetel.component.tree.writer.util.MappingError;
import org.jetel.component.tree.writer.xml.XmlMappingValidator;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.junit.Before;
import org.junit.Test;

/**
 * @author jedlickad (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25. 9. 2017
 */
public class XmlMappingValidatorTest {
	
	private static final String MAPPING = "<?xml version='1.0' encoding='UTF-8'?>"
			+ "<root xmlns:clover='http://www.cloveretl.com/ns/xmlmapping' xmlns:my='http://www.myschema.com'>"
			+ "  <element0 xml:lang='en' clover:inPort='0'>"
			+ "    <field1 my:id='1'>$0.field1</field1>"
			+ "  </element0>"
			+ "</root>";
	
	private TreeWriterMapping mapping;
	XmlMappingValidator validator;
	
	@Before
	public void setUp() throws UnsupportedEncodingException, XMLStreamException {
		mapping = TreeWriterMapping.fromXml(new ByteArrayInputStream(MAPPING.getBytes(TreeWriterMapping.DEFAULT_ENCODING)));
		Map<Integer, DataRecordMetadata> inPorts = new HashMap<>();
		DataRecordMetadata metadata = new DataRecordMetadata("recordName1");
		metadata.addField(new DataFieldMetadata("field1", 10));
		inPorts.put(0, metadata);
		validator = new XmlMappingValidator(inPorts, false);
	}

	@Test
	public void validNamespaceTest() throws UnsupportedEncodingException, XMLStreamException {
		validator.setMapping(mapping);
		validator.validate();
		assertTrue(validator.getErrorsCount() == 0);
	}

	@Test
	public void invalidNamespaceTest() throws XMLStreamException {
		ContainerNode xml = mapping.getRootElement();
		ObjectNode root = (ObjectNode) xml.getChildren().get(0);
		ObjectNode element0 = (ObjectNode) root.getChildren().get(0);
		Attribute wrongAttr = new Attribute(element0);
		wrongAttr.setProperty(MappingProperty.NAME, "xx:id");
		wrongAttr.setProperty(MappingProperty.VALUE, "e1");
		element0.addAttribute(1, wrongAttr);
		validator.setMapping(mapping);
		validator.validate();
		assertTrue(validator.getErrorsCount() == 1);
		assertTrue(validator.getErrorsMap().containsKey(wrongAttr));
	}
}

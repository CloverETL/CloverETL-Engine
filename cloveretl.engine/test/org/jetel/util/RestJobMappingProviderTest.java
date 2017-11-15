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
package org.jetel.util;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.junit.Test;

/**
 * @author adamekl (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 14. 11. 2017
 */
public class RestJobMappingProviderTest {

	private static final String EMPTY_STRING = "";
	
	private static final String NODE_PROPERTY_TOL_LEVEL_ARRAY = "topLevelArray";
	private static final String NODE_PROPERTY_METADATA = "metadataName";	
	
	@Test
	public void testMappingNullParams() {
		assertEquals(EMPTY_STRING, RestJobMappingProvider.createMapping(null, null));
		assertEquals(EMPTY_STRING, RestJobMappingProvider.createMapping(null, RestJobResponseFormat.JSON));
		assertEquals(EMPTY_STRING, RestJobMappingProvider.createMapping(createNode(), null));
		assertEquals(EMPTY_STRING, RestJobMappingProvider.createMapping(fillNode(createNode()), null));
	} 

	@Test
	public void testMappingCsv() {
		assertEquals(EMPTY_STRING, RestJobMappingProvider.createMapping(fillNode(createNode()), RestJobResponseFormat.CSV));
	} 
	
	@Test
	public void testMappingXML() {
		String expecting = 
		"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" +
		"<root xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\">" +
		"	<clover:element clover:inPort=\"0\" clover:name=\"MetadataPort0\">" +
		"		<clover:elements clover:include=\"$0.*\"/>" +
		"	</clover:element>" +
		"	<clover:element clover:inPort=\"2\" clover:name=\"MetadataPort2\">" +
		"		<clover:elements clover:include=\"$2.*\"/>" +
		"	</clover:element>" +
		"	<clover:element clover:inPort=\"3\" clover:name=\"MetadataPort3\">" +
		"		<clover:elements clover:include=\"$3.*\"/>" +
		"	</clover:element>" +
		"</root>";
		
		String mapping = RestJobMappingProvider.createMapping(fillNode(createNode()), RestJobResponseFormat.XML);
		assertEquals(StringUtils.deleteWhitespace(expecting), StringUtils.deleteWhitespace(mapping));
	}
	
	@Test
	public void testMappingJsonWithMetadata() {
		String expecting = 
		"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" + 
		"<root xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\">" + 
		"	<clover:collection clover:inPort=\"0\" clover:name=\"MetadataPort0\">" + 
		"		<element0>" + 
		"			<clover:elements clover:include=\"$0.*\"/>" + 
		"		</element0>" + 
		"	</clover:collection>" + 
		"	<clover:collection clover:inPort=\"2\" clover:name=\"MetadataPort2\">" + 
		"		<element2>" + 
		"			<clover:elements clover:include=\"$2.*\"/>" + 
		"		</element2>" + 
		"	</clover:collection>" + 		
		"	<clover:collection clover:inPort=\"3\" clover:name=\"MetadataPort3\">" + 
		"		<element3>" + 
		"			<clover:elements clover:include=\"$3.*\"/>" + 
		"		</element3>" + 
		"	</clover:collection>" + 
		"</root>";

		String mapping = RestJobMappingProvider.createMapping(fillNode(createNode()), RestJobResponseFormat.JSON);
		assertEquals(StringUtils.deleteWhitespace(expecting), StringUtils.deleteWhitespace(mapping));
	}
	
	@Test
	public void testMappingJsonWithTopLeverArray() {
		String expecting = 
		"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" + 
		"<clover:collection xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\" clover:inPort=\"0\" clover:name=\"root\">" + 
		"	<element0>" + 
		"		<clover:elements clover:include=\"$0.*\"/>" + 
		"	</element0>" + 
		"</clover:collection>"; 
		
		Node node = fillNode(createNode());
		node.getAttributes().setProperty(NODE_PROPERTY_METADATA, Boolean.FALSE.toString());
		node.getAttributes().setProperty(NODE_PROPERTY_TOL_LEVEL_ARRAY, Boolean.TRUE.toString());

		String mapping = RestJobMappingProvider.createMapping(node, RestJobResponseFormat.JSON);
		assertEquals(StringUtils.deleteWhitespace(expecting), StringUtils.deleteWhitespace(mapping));
	}
	
	@Test
	public void testMappingJsonWithoutTopLeverArray() {
		String expecting = 
		"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>" + 
		"<root xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\" clover:inPort=\"0\">" + 
		"	<MetadataPort0Field0>$0.MetadataPort0Field0</MetadataPort0Field0>" + 
		"	<MetadataPort0Field1>$0.MetadataPort0Field1</MetadataPort0Field1>" + 
		"</root>";
		
		Node node = fillNode(createNode());
		node.getAttributes().setProperty(NODE_PROPERTY_METADATA, Boolean.FALSE.toString());
		node.getAttributes().setProperty(NODE_PROPERTY_TOL_LEVEL_ARRAY, Boolean.FALSE.toString());

		String mapping = RestJobMappingProvider.createMapping(node, RestJobResponseFormat.JSON);
		assertEquals(StringUtils.deleteWhitespace(expecting), StringUtils.deleteWhitespace(mapping));
	}
	
	private Node createNode() {
		Node dummyComponent = new Node("DummyProducer") {

			@Override
			public String getType() {
				return "NONE";
			}

			@Override
			protected Result execute() throws Exception {
				return null;
			}
		};
		return dummyComponent;
	}
	
	private Node fillNode(Node node) {
		Properties attributes = new Properties();
		attributes.setProperty(NODE_PROPERTY_METADATA, Boolean.TRUE.toString());
		attributes.setProperty(NODE_PROPERTY_TOL_LEVEL_ARRAY, Boolean.TRUE.toString());
		
		node.setAttributes(attributes);
		
		DataRecordMetadata metadata0 = new DataRecordMetadata("MetadataPort0");
		metadata0.addField(new DataFieldMetadata("MetadataPort0Field0", 1));
		metadata0.addField(new DataFieldMetadata("MetadataPort0Field1", 1));
		Edge inputPort0 = new Edge("0", metadata0);
		node.addInputPort(0, inputPort0);
		
		Edge inputPort2 = new Edge("2", new DataRecordMetadata("MetadataPort2"));
		node.addInputPort(2, inputPort2);
		
		Edge inputPort3 = new Edge("3", new DataRecordMetadata("MetadataPort3"));
		node.addInputPort(3, inputPort3);
		
		return node;
	}
}

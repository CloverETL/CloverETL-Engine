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
package org.jetel.graph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.test.CloverTestCase;
import org.jetel.util.JAXBContextProvider;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23. 8. 2013
 */
public class TransformationGraphXMLReaderWriterTest extends CloverTestCase {

	public void testWriteGraphParameters() throws JAXBException {
		TransformationGraphXMLReaderWriter writer = new TransformationGraphXMLReaderWriter(null);
		
		GraphParameters parameters = new GraphParameters();
		parameters.addGraphParameter("key1", "value1");
		parameters.addGraphParameter("key2", "");
		parameters.addGraphParameter("key3", null);
		parameters.addGraphParameter("key4", "value4").setSecure(true);
		parameters.addGraphParameter("key5", "value5").setSingleType("fileURL");
		parameters.addGraphParameter("key6", "value6").setComponentReference("component1", "fileURL");
		parameters.addGraphParameter("key7", "value7").setPublic(true);
		parameters.addGraphParameter("key8", "value8").setRequired(true);
		
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		writer.writeGraphParameters(parameters, os);

		ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
		
		System.out.println(new String(os.toByteArray()));
		
	    JAXBContext context = JAXBContextProvider.getInstance().getContext(DummyGraphParameters.class);
	    Unmarshaller m = context.createUnmarshaller();
	    
	    DummyGraphParameters result = (DummyGraphParameters) m.unmarshal(is);

		assertTrue(result.parameters.size() == 8);

		assertTrue(result.parameters.get(0).name.equals("key1"));
		assertTrue(result.parameters.get(0).value.equals("value1"));
		assertTrue(result.parameters.get(0).secure == false);
		assertTrue(result.parameters.get(0).singleType == null);
		assertTrue(result.parameters.get(0).componentReference == null);

		assertTrue(result.parameters.get(1).name.equals("key2"));
		assertTrue(result.parameters.get(1).value.equals(""));
		assertTrue(result.parameters.get(1).secure == false);
		assertTrue(result.parameters.get(1).singleType == null);
		assertTrue(result.parameters.get(1).componentReference == null);

		assertTrue(result.parameters.get(2).name.equals("key3"));
		assertTrue(result.parameters.get(2).value.isEmpty());
		assertTrue(result.parameters.get(2).secure == false);
		assertTrue(result.parameters.get(2).singleType == null);
		assertTrue(result.parameters.get(2).componentReference == null);

		assertTrue(result.parameters.get(3).name.equals("key4"));
		assertTrue(result.parameters.get(3).value.equals("value4"));
		assertTrue(result.parameters.get(3).secure == true);
		assertTrue(result.parameters.get(3).singleType == null);
		assertTrue(result.parameters.get(3).componentReference == null);

		assertTrue(result.parameters.get(4).name.equals("key5"));
		assertTrue(result.parameters.get(4).value.equals("value5"));
		assertTrue(result.parameters.get(4).singleType.name.equals("fileURL"));
		assertTrue(result.parameters.get(4).componentReference == null);

		assertTrue(result.parameters.get(5).name.equals("key6"));
		assertTrue(result.parameters.get(5).value.equals("value6"));
		assertTrue(result.parameters.get(5).singleType == null);
		assertTrue(result.parameters.get(5).componentReference.referencedComponent.equals("component1"));
		assertTrue(result.parameters.get(5).componentReference.referencedProperty.equals("fileURL"));
		
		assertTrue(result.parameters.get(6).name.equals("key7"));
		assertTrue(result.parameters.get(6).value.equals("value7"));
		assertTrue(result.parameters.get(6).secure == false);
		assertTrue(result.parameters.get(6).singleType == null);
		assertTrue(result.parameters.get(6).componentReference == null);
		assertTrue(result.parameters.get(6).isPublic == true);
		assertTrue(result.parameters.get(6).required == false);

		assertTrue(result.parameters.get(7).name.equals("key8"));
		assertTrue(result.parameters.get(7).value.equals("value8"));
		assertTrue(result.parameters.get(7).secure == false);
		assertTrue(result.parameters.get(7).singleType == null);
		assertTrue(result.parameters.get(7).componentReference == null);
		assertTrue(result.parameters.get(7).isPublic == false);
		assertTrue(result.parameters.get(7).required == true);
	}

	@XmlRootElement(name = "GraphParameters")
	public static class DummyGraphParameters {

		@XmlElement(name = "GraphParameter")
		public List<DummyGraphParameter> parameters;
	}
	
	@XmlRootElement(name = "GraphParameter")
	public static class DummyGraphParameter {
		@XmlAttribute(name="name")
		public String name;
		@XmlAttribute(name="value")
		public String value;
		@XmlAttribute(name="secure")
		public boolean secure;
		@XmlAttribute(name="public")
		public boolean isPublic;
		@XmlAttribute(name="required")
		public boolean required;
		@XmlElement(name="singleType")
		public SingleType singleType;
		@XmlElement(name="componentReference")
		public ComponentReference componentReference;
	}
	
	public static class SingleType {
		@XmlAttribute(name="name")
		public String name;
	}

	public static class ComponentReference {
		@XmlAttribute(name="referencedComponent")
		public String referencedComponent;
		@XmlAttribute(name="referencedProperty")
		public String referencedProperty;
	}

}

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
		
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		writer.writeGraphParameters(parameters, os);

		ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
		
		
	    JAXBContext context = JAXBContextProvider.getInstance().getContext(DummyGraphParameters.class);
	    Unmarshaller m = context.createUnmarshaller();
	    
	    DummyGraphParameters result = (DummyGraphParameters) m.unmarshal(is);

		assertTrue(result.parameters.size() == 4);

		assertTrue(result.parameters.get(0).name.equals("key1"));
		assertTrue(result.parameters.get(0).value.equals("value1"));
		assertTrue(result.parameters.get(0).secure == false);

		assertTrue(result.parameters.get(1).name.equals("key2"));
		assertTrue(result.parameters.get(1).value.equals(""));
		assertTrue(result.parameters.get(1).secure == false);

		assertTrue(result.parameters.get(2).name.equals("key3"));
		assertTrue(result.parameters.get(2).value == null);
		assertTrue(result.parameters.get(2).secure == false);

		assertTrue(result.parameters.get(3).name.equals("key4"));
		assertTrue(result.parameters.get(3).value.equals("value4"));
		assertTrue(result.parameters.get(3).secure == true);
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
	}
	
}

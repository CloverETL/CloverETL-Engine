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
package org.jetel.component;

import java.sql.SQLException;
import java.util.Properties;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.4.2013
 */
public class ComponentFactoryTest extends CloverTestCase {

	public void testCreateConnection() throws ComponentNotReadyException, JetelException, SQLException {
		Properties properties = new Properties();
		properties.setProperty(IGraphElement.XML_ID_ATTRIBUTE, "DataReader1");
		properties.setProperty(Node.XML_NAME_ATTRIBUTE, "UniversalDataReader1");
		properties.setProperty("fileURL", "./data/data.txt");
		
		TransformationGraph graph = new TransformationGraph();
		
		
		Node dataReader = (Node) ComponentFactory.createComponent(graph, "DATA_READER", properties);
		
		assertTrue(dataReader != null);
		assertEquals("org.jetel.component.DataReader", dataReader.getClass().getName());
		assertEquals("DATA_READER", dataReader.getType());
		assertEquals("UniversalDataReader1", dataReader.getName());
	}

}

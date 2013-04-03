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
package org.jetel.data.lookup;

import java.sql.SQLException;
import java.util.Properties;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.4.2013
 */
public class LookupTableFactoryTest extends CloverTestCase {

	public void testCreateConnection() throws ComponentNotReadyException, JetelException, SQLException {
		Properties properties = new Properties();
		properties.setProperty(IGraphElement.XML_ID_ATTRIBUTE, "LookupTable1");
		properties.setProperty(IGraphElement.XML_NAME_ATTRIBUTE, "SimpleLookupTable1");
		properties.setProperty(IGraphElement.XML_TYPE_ATTRIBUTE, "simpleLookup");
		properties.setProperty("key", "key");
		properties.setProperty("metadata", "metadata");
		
		TransformationGraph graph = new TransformationGraph();
		
		LookupTable simpleLookupTable = (LookupTable) LookupTableFactory.createLookupTable(graph, "simpleLookup", properties);
		
		assertTrue(simpleLookupTable != null);
		assertEquals("org.jetel.lookup.SimpleLookupTable", simpleLookupTable.getClass().getName());
		assertEquals("LookupTable1", simpleLookupTable.getId());
		assertEquals("SimpleLookupTable1", simpleLookupTable.getName());
	}

}

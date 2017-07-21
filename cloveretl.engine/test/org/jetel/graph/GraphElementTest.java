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

import java.util.Properties;

import org.jetel.component.ComponentFactory;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.6.2013
 */
public class GraphElementTest extends CloverTestCase {

	public void testGetPropertyRefResolver() throws GraphConfigurationException {
		TransformationGraph graph = new TransformationGraph();
		Phase phase = new Phase(0);
		graph.addPhase(phase);
		
		Properties componentProperties = new Properties();
		componentProperties.setProperty("id", "simplecopy");
		Node component = ComponentFactory.createComponent(graph, "SIMPLE_COPY", componentProperties);
		phase.addNode(component);
		
		graph.getGraphProperties().setProperty("PROPERTY_KEY", "PropertyValue");
		
		assertEquals("PropertyValue", graph.getPropertyRefResolver().resolveRef("${PROPERTY_KEY}"));
		assertEquals("PropertyValue", component.getPropertyRefResolver().resolveRef("${PROPERTY_KEY}"));
	}
	
}

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
package org.jetel.data.sequence;

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
public class SequenceFactoryTest extends CloverTestCase {

	public void testCreateSequence() throws ComponentNotReadyException, JetelException, SQLException {
		Properties properties = new Properties();
		properties.setProperty(IGraphElement.XML_ID_ATTRIBUTE, "SimpleSequence1");
		properties.setProperty(IGraphElement.XML_NAME_ATTRIBUTE, "Simple sequence");
		properties.setProperty("fileURL", "data.txt");
		properties.setProperty("start", "1");
		properties.setProperty("step", "1");
		properties.setProperty("cached", "10");
		
		TransformationGraph graph = new TransformationGraph();
		
		
		Sequence simpleSequence = (Sequence) SequenceFactory.createSequence(graph, "SIMPLE_SEQUENCE", properties);
		
		assertTrue(simpleSequence != null);
		assertEquals("org.jetel.sequence.SimpleSequence", simpleSequence.getClass().getName());
		assertEquals("SimpleSequence1", simpleSequence.getId());
		assertEquals("Simple sequence", simpleSequence.getName());
	}

}

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
package org.jetel.graph.parameter;

import org.jetel.graph.TransformationGraph;
import org.jetel.test.CloverTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31. 10. 2014
 */
public class GraphParameterDynamicValueProviderTest extends CloverTestCase {

	@Before
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}

	@After
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	/**
	 * Test method for {@link org.jetel.graph.parameter.GraphParameterDynamicValueProvider#getValue()}.
	 */
	@Test
	public void testGetValue() {
		{
			// CLO-5131
			String ctl = "//#CTL2\n" +
						 "function string getValue() {\n" +
						 "	 return \"\\\"title\\\"\";\n" +
						 "}";
			TransformationGraph graph = new TransformationGraph();
			GraphParameterDynamicValueProvider provider = GraphParameterDynamicValueProvider.create(graph, "A", ctl);
			String actual = provider.getValue();
			assertEquals("\"title\"", actual);
			assertEquals(7, actual.length());
		}
	}

}

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
package org.jetel.util.primitive;

import org.jetel.graph.GraphParameters;
import org.jetel.test.CloverTestCase;
import org.jetel.util.property.RefResFlag;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11. 10. 2013
 */
public class TypedPropertiesTest extends CloverTestCase {

	public void test() {
		GraphParameters graphParameters = new GraphParameters();
		graphParameters.addGraphParameter("a", "valueA");
		graphParameters.addGraphParameter("b", "${a} valueB");
		graphParameters.addGraphParameter("c", "${a} \\user");
		
		TypedProperties properties = graphParameters.asProperties();
		assertEquals("valueA valueB", properties.getStringProperty("b"));
		assertEquals("valueA \\user", properties.getStringProperty("c", null, RefResFlag.SPEC_CHARACTERS_OFF));
	}
	
}

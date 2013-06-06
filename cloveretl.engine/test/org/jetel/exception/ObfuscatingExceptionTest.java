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
package org.jetel.exception;

import org.jetel.graph.ContextProvider;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.PrimitiveAuthorityProxy;
import org.jetel.test.CloverTestCase;
import org.jetel.util.ExceptionUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.6.2013
 */
public class ObfuscatingExceptionTest extends CloverTestCase {

	public void test() {
		Exception e = new Exception("xxx pass1", new Exception("yyy pass2"));
		
		TransformationGraph graph = new TransformationGraph();
		graph.getRuntimeContext().setAuthorityProxy(new PrimitiveAuthorityProxy(){
			@Override
			public String obfuscateSecureParameters(String text) {
				text = text.replaceAll("pass1", "\\${SECURE_PASSWORD1}");
				text = text.replaceAll("pass2", "\\${SECURE_PASSWORD2}");
				return text;
			}
		});
		Context c = ContextProvider.registerGraph(graph);
		try {
			e = new ObfuscatingException(e);
			assertEquals("xxx ${SECURE_PASSWORD1}\n yyy ${SECURE_PASSWORD2}", ExceptionUtils.getMessage(e));
		} finally {
			ContextProvider.unregister(c);
		}
		assertEquals("xxx ${SECURE_PASSWORD1}\n yyy ${SECURE_PASSWORD2}", ExceptionUtils.getMessage(e));
	}
	
}

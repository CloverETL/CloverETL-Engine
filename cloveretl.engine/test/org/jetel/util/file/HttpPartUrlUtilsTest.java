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
package org.jetel.util.file;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author reichman (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created May 5, 2017
 */
public class HttpPartUrlUtilsTest {
	
	@Test
	public void testIsRequestUrl() {
		
		assertTrue(HttpPartUrlUtils.isRequestUrl("request:body"));
		assertTrue(HttpPartUrlUtils.isRequestUrl("request:part:name"));
		assertFalse(HttpPartUrlUtils.isRequestUrl((String)null));
		assertFalse(HttpPartUrlUtils.isRequestUrl("request"));
		assertFalse(HttpPartUrlUtils.isRequestUrl("response:body"));
		assertFalse(HttpPartUrlUtils.isRequestUrl("dict:name"));
		assertFalse(HttpPartUrlUtils.isRequestUrl("sandbox://test/local/graph/graph.grf"));
	}
	
	@Test
	public void testIsResponseUrl() {
		
		assertTrue(HttpPartUrlUtils.isResponseUrl("response:body"));
		assertFalse(HttpPartUrlUtils.isResponseUrl((String)null));
		assertFalse(HttpPartUrlUtils.isResponseUrl("response"));
		assertFalse(HttpPartUrlUtils.isResponseUrl("request:body"));
		assertFalse(HttpPartUrlUtils.isResponseUrl("request:part:name"));
		assertFalse(HttpPartUrlUtils.isResponseUrl("dict:name"));
		assertFalse(HttpPartUrlUtils.isResponseUrl("sandbox://test/local/graph/graph.grf"));
	}
}

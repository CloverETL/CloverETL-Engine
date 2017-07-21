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

import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21. 10. 2013
 */
public class JobTypeTest extends CloverTestCase {

	public void testFromFileExtension() {
		assertEquals(JobType.ETL_GRAPH, JobType.fromFileExtension(null));
		assertEquals(JobType.ETL_GRAPH, JobType.fromFileExtension(""));
		assertEquals(JobType.ETL_GRAPH, JobType.fromFileExtension("grf"));
		assertEquals(JobType.JOBFLOW, JobType.fromFileExtension("jbf"));
		assertEquals(JobType.PROFILER_JOB, JobType.fromFileExtension("cpj"));
		assertEquals(JobType.SUBGRAPH, JobType.fromFileExtension("sgrf"));
		assertEquals(JobType.SUBJOBFLOW, JobType.fromFileExtension("sjbf"));
		try {
			JobType.fromFileExtension("scpj");
			fail();
		} catch (IllegalArgumentException e) {
			//ok
		}
	}
	
}

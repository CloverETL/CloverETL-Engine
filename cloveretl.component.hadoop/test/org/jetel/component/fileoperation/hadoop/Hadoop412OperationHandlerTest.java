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
package org.jetel.component.fileoperation.hadoop;

import java.net.URI;

public class Hadoop412OperationHandlerTest extends HadoopOperationHandlerTest {

	@Override
	protected URI getTestingURI() {
		return URI.create(CDH412);
	}

	/*
	 * Used for testing MOVE between two servers.
	 */
	@Override
	protected URI getRemoteURI() {
		return URI.create(CDH_3U5);
	}

	@Override
	public void testInterruptDelete() throws Exception {
		// FIXME disabled - takes too long in Jenkins
	}

	@Override
	public void testInterruptCopy() throws Exception {
		// FIXME disabled - takes too long in Jenkins
	}

	@Override
	public void testInterruptMove() throws Exception {
		// FIXME disabled - takes too long in Jenkins
	}

}

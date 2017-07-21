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
package org.jetel.graph.runtime.tracker;



import org.jetel.data.DataRecordFactory;
import org.jetel.data.Token;
import org.jetel.graph.Node;
import org.junit.Test;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 28.5.2012
 */
public class ComplexComponentTokenTrackerTest extends AbstractComponentTokenTrackerTestBase {

	@Override
	protected ComponentTokenTracker createComponentTokenTracker(Node testComponent) {
		return new ComplexComponentTokenTracker(testComponent);
	}
	
	@Test
	public void test1toN_Order() {
		Token inToken = createAndReadRecord(null, 0);
		
		int port = 0;
		Token outToken = writeToken(port);
		assertInitLinkWrite(inToken, port, outToken);
		outToken = writeToken(++port);
		assertInitLinkWrite(inToken, port, outToken);
		outToken = writeToken(++port);
		assertInitLinkWrite(inToken, port, outToken);
		outToken = writeToken(++port);
		assertInitLinkWrite(inToken, port, outToken);
		
		Token inToken2 = createAndReadRecord(inToken, 0);
		
		outToken = writeToken(0);
		assertInitLinkWrite(inToken2, 0, outToken);

		inEdgesEof(inToken2);
	}

	@Test
	public void test1toN_NoOrder() {
		Token inToken = createAndReadRecord(null, 0);

		Token outToken = writeToken(2);
		assertInitLinkWrite(inToken, 2, outToken);
		outToken = writeToken(1);
		assertInitLinkWrite(inToken, 1, outToken);
		outToken = writeToken(3);
		assertInitLinkWrite(inToken, 3, outToken);
		outToken = writeToken(0);
		assertInitLinkWrite(inToken, 0, outToken);
		
		Token inToken2 = createAndReadRecord(inToken, 1);
		
		outToken = writeToken(3);
		assertInitLinkWrite(inToken2, 3, outToken);

		inToken = createAndReadRecord(inToken2, 2);
		
		outToken = writeToken(0);
		assertInitLinkWrite(inToken, 0, outToken);
		
		inEdgesEof(inToken);
	}
	
	@Test
	public void test1to0_inPort0() {
		Token inToken = createAndReadRecord(null, 0);
		inEdgesEof(inToken);
	}
	
	@Test
	public void test1to0_inPort2() {
		Token inToken = DataRecordFactory.newToken("in0");
		tokenTracker.initToken(dummyComponent, inToken);
		int actionsCountBefore = tokenTracker.getActionsCount();
		readToken(2, inToken);
		assertReadToken(null, inToken, 2, actionsCountBefore);

		inEdgesEof(inToken);
	}
	
}

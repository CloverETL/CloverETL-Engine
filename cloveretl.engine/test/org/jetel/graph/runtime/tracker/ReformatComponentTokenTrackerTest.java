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

import org.jetel.data.Token;
import org.jetel.graph.Node;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31.5.2012
 */
public class ReformatComponentTokenTrackerTest extends ComplexComponentTokenTrackerTest {

	private Token lastOutToken;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		lastOutToken = null;
	}
	
	@Override
	protected ComponentTokenTracker createComponentTokenTracker(Node testComponent) {
		return new ReformatComponentTokenTracker(testComponent);
	}
	
	@Override
	protected void assertReadToken(Token lastInToken, Token currentInToken, int currentInPort, int actionsCountBefore) {
		tokenTracker.assertLastActions(actionsCountBefore, new TokenAction(TokenActionType.READ, testComponent, currentInPort, currentInToken));
	}
	
	@Override
	protected void assertInitLinkWrite(Token inToken, int outPort, Token outToken) {
		if (lastOutToken != outToken) {
			tokenTracker.assertLastActions(new TokenAction(TokenActionType.WRITE, testComponent, outPort, outToken));
			lastOutToken = outToken;
		} else {
			super.assertInitLinkWrite(inToken, outPort, outToken);
		}
	}
	
	@Override
	protected void assertActionsAfterInEdgesEof(Token lastReadToken, int actionsCountBeforeFinish) {
		if (lastReadToken != null && (lastOutToken == null || lastReadToken.getTokenId() != lastOutToken.getTokenId())) {
			tokenTracker.assertLastActions(actionsCountBeforeFinish, new TokenAction(TokenActionType.FREE, testComponent, lastReadToken));
		} else {
			assertEquals(actionsCountBeforeFinish, tokenTracker.getActionsCount());
		}
	}
	
}

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
import org.junit.Test;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31.5.2012
 */
public class CopyComponentTokenTrackerTest extends AbstractComponentTokenTrackerTestBase {

	private Token[] inTokens = new Token[4];

	@Override
	protected ComponentTokenTracker createComponentTokenTracker(Node testComponent) {
		return new CopyComponentTokenTracker(testComponent);
	}
	
	@Override
	protected void assertReadToken(Token lastInToken, Token currentInToken, int currentInPort, int actionsCountBefore) {
		tokenTracker.assertLastActions(
				actionsCountBefore,
				new TokenAction(TokenActionType.READ, testComponent, currentInPort, currentInToken)
		);
	}
	
	@Test
	public void test1toN_differentInOutOrder() {
		for (int i = 0; i < inTokens.length; i++) {
			inTokens[i] = createAndReadRecord(i > 0 ? inTokens[i-1] : null, 0);
		}
		
		for (int i = 0; i < inTokens.length; i++) {
			int actionsCount = tokenTracker.getActionsCount();
			Token outToken = writeToken(2, inTokens[i]);
			tokenTracker.assertLastActions(actionsCount, new TokenAction(TokenActionType.WRITE, testComponent, 2, outToken));
			
			outToken = writeToken(0, inTokens[i].duplicate());
			assertInitLinkWrite(inTokens[i], 0, outToken);
	
			outToken = writeToken(1, inTokens[i].duplicate());
			assertInitLinkWrite(inTokens[i], 1, outToken);
			
			outToken = writeToken(3, inTokens[i].duplicate());
			assertInitLinkWrite(inTokens[i], 3, outToken);
		}
		
		inEdgesEof(inTokens[inTokens.length - 1]);
	}
	
	@Override
	protected void assertActionsAfterInEdgesEof(Token lastReadToken, int actionsCountBeforeFinish) {
		assertEquals(actionsCountBeforeFinish, tokenTracker.getActionsCount());
	}
	
}

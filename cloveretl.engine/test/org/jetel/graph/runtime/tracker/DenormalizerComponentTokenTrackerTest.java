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
 * @created 1.6.2012
 */
public class DenormalizerComponentTokenTrackerTest extends AbstractComponentTokenTrackerTestBase {

	private Token[] inTokens = new Token[4];

	@Override
	protected ComponentTokenTracker createComponentTokenTracker(Node testComponent) {
		return new DenormalizerComponentTokenTracker(testComponent);
	}

	@Test
	public void test() {
		for (int i = 0; i < inTokens.length; i++) {
			inTokens[i] = createAndReadRecord(i > 0 ? inTokens[i-1] : null, i);
		}
		
		int actionsCount = tokenTracker.getActionsCount();
		
		Token outToken = writeToken(0);
		
		tokenTracker.assertLastActions(
				actionsCount,
				new TokenAction(TokenActionType.INIT, testComponent, outToken),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[0], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[0]),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[1], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[1]),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[2], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[2]),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[3], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[3]),
				new TokenAction(TokenActionType.WRITE, testComponent, 0, outToken)
			);
		
		inEdgesEof(inTokens[inTokens.length - 1]);
	}
	
	@Test
	public void testWithCache() {
		componentTokenTracker = new DenormalizerComponentTokenTracker(testComponent, 2);
		
		for (int i = 0; i < inTokens.length; i++) {
			inTokens[i] = createAndReadRecord(i > 0 ? inTokens[i-1] : null, inTokens.length - i - 1);
		}
		
		int actionsCount = tokenTracker.getActionsCount();
		Token outToken = writeToken(0);
		
		tokenTracker.assertLastActions(
				actionsCount,
				new TokenAction(TokenActionType.INIT, testComponent, outToken),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[0], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[0]),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[1], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[1]),
				new TokenAction(TokenActionType.WRITE, testComponent, 0, outToken)
			);
		
		inTokens[0] = createAndReadRecord(inTokens[3], 2);
		inTokens[1] = createAndReadRecord(inTokens[0], 1);

		actionsCount = tokenTracker.getActionsCount();
		outToken = writeToken(0);
		
		tokenTracker.assertLastActions(
				actionsCount,
				new TokenAction(TokenActionType.INIT, testComponent, outToken),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[2], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[2]),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[3], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[3]),
				new TokenAction(TokenActionType.WRITE, testComponent, 0, outToken)
			);
		
		inTokens[2] = createAndReadRecord(inTokens[1], 0);
		
		inEdgesEof(inTokens[inTokens.length - 1]);
		
		actionsCount = tokenTracker.getActionsCount();
		outToken = writeToken(0);
		
		tokenTracker.assertLastActions(
				actionsCount,
				new TokenAction(TokenActionType.INIT, testComponent, outToken),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[0], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[0]),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[1], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[1]),
				new TokenAction(TokenActionType.LINK, testComponent, inTokens[2], outToken),
				new TokenAction(TokenActionType.FREE, testComponent, inTokens[2]),
				new TokenAction(TokenActionType.WRITE, testComponent, 0, outToken)
			);
	}

	@Override
	protected void assertReadToken(Token lastInToken, Token currentInToken, int currentInPort, int actionsCountBefore) {
		tokenTracker.assertLastActions(
				actionsCountBefore,
				new TokenAction(TokenActionType.READ, testComponent, currentInPort, currentInToken)
		);
	}
	
	@Override
	protected void assertActionsAfterInEdgesEof(Token lastReadToken, int actionsCountBeforeFinish) {
		assertEquals(actionsCountBeforeFinish, tokenTracker.getActionsCount());
	}
}

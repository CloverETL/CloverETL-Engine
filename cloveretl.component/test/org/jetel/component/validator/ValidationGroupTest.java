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
package org.jetel.component.validator;

import org.jetel.component.validator.ValidationGroup.Conjunction;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.data.DataRecord;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 27.1.2013
 */
public class ValidationGroupTest extends ValidatorTestCase {

	@Test
	public void testNameablity() {
		testNameability(ValidationGroup.class);
	}
	@Test
	public void testDisablity() {
		testDisability(ValidationGroup.class);
	}
	@Test
	public void testConjunction() {
		ValidationGroup group = new ValidationGroup();
		assertEquals(Conjunction.AND, group.getConjunction());
		group.setConjunction(Conjunction.OR);
		assertEquals(Conjunction.OR, group.getConjunction());
		group.setConjunction(Conjunction.AND);
		assertEquals(Conjunction.AND, group.getConjunction());
		group.setConjunction(null);
		assertNotNull(group.getConjunction());
	}
	@Test
	public void testReadyness() {
		ValidationGroup group = new ValidationGroup();
		assertTrue(group.isReady());
		group.addChild(new AlwaysNotReadyRule());
		assertFalse(group.isReady());
		group.addChild(new AlwaysNotReadyRule());
		assertFalse(group.isReady());
		
		group = new ValidationGroup();
		group.addChild(new AlwaysReadyRule());
		assertTrue(group.isReady());
		group.addChild(new AlwaysReadyRule());
		assertTrue(group.isReady());
		group.addChild(new AlwaysNotReadyRule());
		assertFalse(group.isReady());
	}
	@Test
	public void testValidating() {
		ValidationGroup group = new ValidationGroup();
		assertEquals(State.NOT_VALIDATED, group.isValid(null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null));
		group.addChild(new AlwaysValidRule());
		assertEquals(State.INVALID, group.isValid(null, null));
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID, group.isValid(null, null));
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID, group.isValid(null, null));
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.AND);
		group.addChild(new AlwaysNotValidatedRule());
		assertEquals(State.VALID, group.isValid(null, null));
		group.setConjunction(Conjunction.OR);
		assertEquals(State.INVALID, group.isValid(null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.AND);
		group.addChild(new AlwaysNotValidatedRule());
		assertEquals(State.VALID, group.isValid(null, null));
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.AND);
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID, group.isValid(null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		group.addChild(new AlwaysNotValidatedRule());
		assertEquals(State.INVALID, group.isValid(null, null));
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID, group.isValid(null, null));
	}
	@Test
	public void testLaziness() {
		
	}
	@Test
	public void testPrelimitaryCondition() {
		
	}
	
	/* Some mock objects */
	private class AlwaysNotReadyRule extends AbstractValidationRule {
		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
			return null;
		}
		@Override
		public boolean isReady() {
			return false;
		}
	}
	private class AlwaysReadyRule extends AbstractValidationRule {
		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
			return null;
		}
		@Override
		public boolean isReady() {
			return true;
		}
	}
	private class AlwaysValidRule extends AbstractValidationRule {
		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
			return State.VALID;
		}
		@Override
		public boolean isReady() {
			return true;
		}
	}
	private class AlwaysInvalidRule extends AbstractValidationRule {
		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
			return State.INVALID;
		}
		@Override
		public boolean isReady() {
			return true;
		}
	}
	private class AlwaysNotValidatedRule extends AbstractValidationRule {
		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea) {
			return State.NOT_VALIDATED;
		}
		@Override
		public boolean isReady() {
			return true;
		}
	}
}

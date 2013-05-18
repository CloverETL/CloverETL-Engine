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

import java.util.List;

import org.jetel.component.validator.ValidationGroup.Conjunction;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;
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
	public void testCommon() {
		testCommon(ValidationGroup.class);
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
		assertTrue(group.isReady(null, null, null));
		group.addChild(new AlwaysNotReadyRule());
		assertFalse(group.isReady(null, null, null));
		group.addChild(new AlwaysNotReadyRule());
		assertFalse(group.isReady(null, null, null));
		
		group = new ValidationGroup();
		group.addChild(new AlwaysReadyRule());
		assertTrue(group.isReady(null, null, null));
		group.addChild(new AlwaysReadyRule());
		assertTrue(group.isReady(null, null, null));
		group.addChild(new AlwaysNotReadyRule());
		assertFalse(group.isReady(null, null, null));
	}
	@Test
	public void testValidating() {
		ValidationGroup group = new ValidationGroup();
		group.setEnabled(true);
		assertEquals(State.NOT_VALIDATED, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null, null));
		group.addChild(new AlwaysValidRule());
		assertEquals(State.INVALID, group.isValid(null, null, null));
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null, null));
		group.addChild(new AlwaysNotValidatedRule());
		assertEquals(State.INVALID, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID, group.isValid(null, null, null));
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID, group.isValid(null, null, null));
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.AND);
		assertEquals(State.NOT_VALIDATED, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		assertEquals(State.NOT_VALIDATED, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.AND);
		group.addChild(new AlwaysNotValidatedRule());
		assertEquals(State.NOT_VALIDATED, group.isValid(null, null, null));
		group.setConjunction(Conjunction.OR);
		assertEquals(State.NOT_VALIDATED, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.AND);
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.AND);
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.INVALID, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysNotValidatedRule());
		group.addChild(new AlwaysNotValidatedRule());
		assertEquals(State.NOT_VALIDATED, group.isValid(null, null, null));
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		group.addChild(new AlwaysInvalidRule());
		group.addChild(new AlwaysInvalidRule());
		group.addChild(new AlwaysValidRule());
		group.addChild(new AlwaysInvalidRule());
		assertEquals(State.VALID, group.isValid(null, null, null));
	}
	@Test
	public void testLaziness() {
		ValidationGroup group = new ValidationGroup();
		group.setEnabled(true);
		group.addChild(new AlwaysInvalidRule());
		CountingRule.resetCounter();
		group.isValid(null, null, null);
		assertEquals(1,CountingRule.getCounter());
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.addChild(new AlwaysInvalidRule());
		group.addChild(new AlwaysInvalidRule());
		group.addChild(new AlwaysInvalidRule());
		CountingRule.resetCounter();
		group.isValid(null, null, null);
		assertEquals(1,CountingRule.getCounter());
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.addChild(new AlwaysValidRule());
		group.addChild(new AlwaysInvalidRule());
		group.addChild(new AlwaysInvalidRule());
		CountingRule.resetCounter();
		group.isValid(null, null, null);
		assertEquals(2,CountingRule.getCounter());
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		group.addChild(new AlwaysValidRule());
		group.addChild(new AlwaysValidRule());
		group.addChild(new AlwaysValidRule());
		CountingRule.resetCounter();
		group.isValid(null, null, null);
		assertEquals(1,CountingRule.getCounter());
		
		group = new ValidationGroup();
		group.setEnabled(true);
		group.setConjunction(Conjunction.OR);
		group.addChild(new AlwaysInvalidRule());
		group.addChild(new AlwaysValidRule());
		CountingRule.resetCounter();
		group.isValid(null, null, null);
		assertEquals(2,CountingRule.getCounter());
	}
	@Test
	public void testPrelimitaryCondition() {
		ValidationGroup group = new ValidationGroup();
		group.setEnabled(true);
		group.setPrelimitaryCondition(new AlwaysValidRule());
		group.addChild(new AlwaysValidRule());
		assertEquals(State.VALID,group.isValid(null, null, null));
		
		group.setPrelimitaryCondition(new AlwaysInvalidRule());
		assertEquals(State.NOT_VALIDATED,group.isValid(null, null, null));
		
		group.setPrelimitaryCondition(new AlwaysNotValidatedRule());
		assertEquals(State.VALID,group.isValid(null, null, null));
	}
	
	/* Some mock objects */
	private static abstract class DummyRule extends AbstractValidationRule {

		@Override
		public TARGET_TYPE getTargetType() {
			return null;
		}
		@Override
		public String getCommonName() {
			return null;
		}
		@Override
		public String getCommonDescription() {
			return null;
		}
		@Override
		protected List<ValidationParamNode> initialize(DataRecordMetadata inMetadata, GraphWrapper graphWrapper) {
			return null;
		}
	}
	
	private static abstract class CountingRule extends DummyRule {
		protected static int counter = 0;
		
		public static int getCounter() {
			return counter;	
		}
		
		public static void resetCounter() {
			counter = 0;
		}
		
	}
	private class AlwaysNotReadyRule extends DummyRule {

		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
			return null;
		}

		@Override
		public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
			return false;
		}

	}
	private class AlwaysReadyRule extends DummyRule {

		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
			return null;
		}

		@Override
		public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
			return true;
		}

	}
	private class AlwaysValidRule extends CountingRule {

		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
			counter++;
			return State.VALID;
		}
		@Override
		public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
			return true;
		}
	}
	private class AlwaysInvalidRule extends CountingRule {

		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
			counter++;
			return State.INVALID;
		}

		@Override
		public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
			return true;
		}

	}
	private class AlwaysNotValidatedRule extends CountingRule {

		@Override
		public State isValid(DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
			counter++;
			return State.NOT_VALIDATED;
		}

		@Override
		public boolean isReady(DataRecordMetadata inputMetadata, ReadynessErrorAcumulator accumulator, GraphWrapper graphWrapper) {
			return true;
		}

	}
}

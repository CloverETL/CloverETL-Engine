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
package org.jetel.component.validator.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.CustomRule;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.ValidationNode.State;
import org.jetel.component.validator.params.LanguageSetting;
import org.jetel.component.validator.utils.CommonFormats;
import org.jetel.data.DataRecord;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.property.PropertyRefResolver;

/**
 * Shared helpers for validator test. It creates support for testing rules outside graph (resolving etc).
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 26.1.2013
 */
public class ValidatorTestCase extends CloverTestCase {
	
	@Override
	public void setUp() {
		initEngine();
	}
	/**
	 * Tests that validation node can hold name
	 * @param testTarget Class to be tested
	 */
	protected void testNameability(Class<? extends ValidationNode> testTarget) {
		ValidationNode rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertNotNull(rule);
		assertEquals(rule.getName(), rule.getCommonName());
		String testName1 = "Test name 1";
		String testName2 = "Test name 2";
		rule.setName(testName1);
		assertEquals(testName1, rule.getName());
		rule.setName(testName2);
		assertEquals(testName2, rule.getName());
	}
	
	/**
	 * Tests that validation node can be disabled and is NOT_VALIDATED when disabled
	 * @param testTarget Class to be tested
	 */
	protected void testDisability(Class<? extends ValidationNode> testTarget) {
		ValidationNode rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertTrue(rule.isEnabled());
		rule.setEnabled(false);
		assertFalse(rule.isEnabled());
		assertEquals(State.NOT_VALIDATED, rule.isValid(TestDataRecordFactory.newRecord(), null, null));
		assertFalse(rule.isEnabled());
	}
	
	/**
	 * Tests common interface from {@link ValidationNode} and {@link AbstractValidationRule}
	 * @param testTarget Class to be tested
	 */
	protected void testCommon(Class<? extends ValidationNode> testTarget) {
		ValidationNode rule;
		try {
			rule = testTarget.newInstance();
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return;
		}
		assertTrue("Common name of rule can't be null.", rule.getCommonName() != null);
		assertTrue("Common name of rule must be non empty.", rule.getCommonName().length() > 0);
		
		assertTrue("Common description of rule can't be null.", rule.getCommonDescription() != null);
		assertTrue("Common description of rule must be non empty.", rule.getCommonDescription().length() > 0);
		
		assertNull(rule.getParentLanguageSetting());
		LanguageSetting ls = new LanguageSetting(true);
		rule.setParentLanguageSetting(ls);
		assertSame(ls, rule.getParentLanguageSetting());
		
		if(rule instanceof AbstractValidationRule) {
			assertNotNull(((AbstractValidationRule) rule).getTargetType());
		}
	}
	
	/**
	 * Return prepared instance of validation rule (language setting and property resolver).
	 * 
	 * @param testTarget Class to be instantiated and prepared
	 * @return Instantiated validation rule (fixed language settings and dummy property resolver).
	 */
	protected <T extends ValidationNode> T createRule(Class<T> testTarget) {
		T rule;
		try {
			rule = testTarget.newInstance();
			rule.setParentLanguageSetting(defaultLanguageSetting());
			rule.setPropertyRefResolver(new DummyGraphWrapper());
			return rule;
		} catch (Exception ex) {
			fail("Cannot create instance of rule/group.");
			return null;
		}
	}
	
	/**
	 * Checks whether given validation rule is valid on given record
	 * @param rule
	 * @param record
	 * @param ea
	 * @param graphWrapper
	 */
	protected void assertValid(ValidationNode rule, DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		assertState(State.VALID, rule, record, ea, graphWrapper);
	}
	/**
	 * @see #assertValid(ValidationNode, DataRecord, ValidationErrorAccumulator, GraphWrapper)
	 */
	protected void assertValid(ValidationNode rule, DataRecord record, ValidationErrorAccumulator ea) {
		assertState(State.VALID, rule, record, ea, null);
	}
	/**
	 * @see #assertValid(ValidationNode, DataRecord, ValidationErrorAccumulator, GraphWrapper)
	 */
	protected void assertValid(ValidationNode rule, DataRecord record) {
		assertState(State.VALID, rule, record, null, null);
	}
	
	/**
	 * Checks whether given validation rule is valid on given record
	 * @param rule
	 * @param record
	 * @param ea
	 * @param graphWrapper
	 */
	protected void assertInvalid(ValidationNode rule, DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		assertState(State.INVALID, rule, record, ea, graphWrapper);
	}
	/**
	 * @see #assertInvalid(ValidationNode, DataRecord, ValidationErrorAccumulator, GraphWrapper)
	 */
	protected void assertInvalid(ValidationNode rule, DataRecord record, ValidationErrorAccumulator ea) {
		assertState(State.INVALID, rule, record, ea, null);
	}
	/**
	 * @see #assertInvalid(ValidationNode, DataRecord, ValidationErrorAccumulator, GraphWrapper)
	 */
	protected void assertInvalid(ValidationNode rule, DataRecord record) {
		assertState(State.INVALID, rule, record, null, null);
	}
	/**
	 * Checks whether given validation rule is valid on given record
	 * @param rule
	 * @param record
	 * @param ea
	 * @param graphWrapper
	 */
	protected void assertNotValidated(ValidationNode rule, DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		assertState(State.NOT_VALIDATED, rule, record, ea, graphWrapper);
	}
	/**
	 * @see #assertNotValidated(ValidationNode, DataRecord, ValidationErrorAccumulator, GraphWrapper)
	 */
	protected void assertNotValidated(ValidationNode rule, DataRecord record, ValidationErrorAccumulator ea) {
		assertState(State.NOT_VALIDATED, rule, record, ea, null);
	}
	/**
	 * @see #assertNotValidated(ValidationNode, DataRecord, ValidationErrorAccumulator, GraphWrapper)
	 */
	protected void assertNotValidated(ValidationNode rule, DataRecord record) {
		assertState(State.NOT_VALIDATED, rule, record, null, null);
	}
	
	private void assertState(ValidationNode.State state, ValidationNode rule, DataRecord record, ValidationErrorAccumulator ea, GraphWrapper graphWrapper) {
		if(ea == null) {
			ea = new ValidationErrorAccumulator();
		}
		if(graphWrapper == null) {
			graphWrapper = new DummyGraphWrapper();
		}
		assertEquals(state, rule.isValid(record, ea, graphWrapper));
		if(state == ValidationNode.State.INVALID && ea.isEmpty()) {
			fail("When rule is INVALID at least one validation error expected in error accumulator.");
		}
	}
	
	protected void assertReadyness(boolean expected, ValidationNode rule, DataRecordMetadata inputMetadata, ReadynessErrorAcumulator rea, GraphWrapper graphWrapper) {
		if(rea == null) {
			rea = new ReadynessErrorAcumulator();
		}
		if(graphWrapper == null) {
			graphWrapper = new DummyGraphWrapper();
		}
		assertEquals(expected, rule.isReady(inputMetadata, rea, graphWrapper));
		if(!expected && rea.getErrors().isEmpty()) {
			fail("When rule is NOT READY at least one readyness error expected in error accumulator.");
		}
	}
	
	protected void assertReadyness(boolean expected, ValidationNode rule, DataRecordMetadata inputMetadata, ReadynessErrorAcumulator rea) {
		assertReadyness(expected, rule, inputMetadata, rea, null);
	}
	protected void assertReadyness(boolean expected, ValidationNode rule, DataRecordMetadata inputMetadata) {
		assertReadyness(expected, rule, inputMetadata, null, null);
	}
	
	/**
	 * Creates fixed language settings for purpose of testing (prevent locale issues when performing tests).
	 * @return Language settings for en.US and UTC
	 */
	protected LanguageSetting defaultLanguageSetting() {
		LanguageSetting ls = new LanguageSetting();
		ls.getLocale().setValue("en.US");
		ls.getNumberFormat().setValue(CommonFormats.NUMBER);
		ls.getDateFormat().setValue("yyyy-MM-dd HH:mm:ss");
		ls.getTimezone().setValue("UTC");
		return ls;
	}
	
	protected Date getDate(String input, String timezone) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
		try {
			return dateFormat.parse(input);
		} catch (ParseException e) {
			return null;
		}
	}
	
	protected Decimal getDecimal(String input) {
		return DecimalFactory.getDecimal(input);
	}
	
	/**
	 * Just an shorter alias to make test readable.
	 */
	protected static class RF extends TestDataRecordFactory {}
	
	/**
	 * Mock graph wrapper providing: dummy PropertyRefResolver, lookup table acess, node paths...
	 */
	protected class DummyGraphWrapper implements GraphWrapper {
		
		private Map<String, LookupTable> lookupTables = new HashMap<String, LookupTable>();
		
		private Map<Integer, CustomRule> customRules = new HashMap<Integer, CustomRule>();
		
		public DummyGraphWrapper(){}
		
		public void addLookupTable(LookupTable toAdd) {
			lookupTables.put(toAdd.getId(), toAdd);
		}
		public void addCustomRule(int id, CustomRule toAdd) {
			customRules.put(Integer.valueOf(id), toAdd);
		}

		@Override
		public List<String> getLookupTables() {
			return new ArrayList<String>(lookupTables.keySet());
		}

		@Override
		public LookupTable getLookupTable(String name) {
			return lookupTables.get(name);
		}

		@Override
		public PropertyRefResolver getRefResolver() {
			return new PropertyRefResolver() {
				@Override
				public String resolveRef(String value) {
					return value;
				}
			};
		}

		@Override
		public void init(ValidationGroup group) {}

		@Override
		public List<String> getNodePath(ValidationNode needle) {
			return Arrays.asList("Example","Path","To","Rule");
		}

		@Override
		public Map<Integer, CustomRule> getCustomRules() {
			return customRules;
		}

		@Override
		public TransformationGraph getTransformationGraph() {
			return new TransformationGraph("dummy");
		}
	}
}

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
package org.jetel.component.validator.rules;

import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.component.validator.rules.LookupValidationRule.POLICY;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.lookup.SimpleLookupTable;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 19.5.2013
 */
public class LookupValidationRuleTest extends ValidatorTestCase {
	
	@Test
	public void testNameability() {
		testNameability(LookupValidationRule.class);
	}
	
	@Test
	public void testDisability() {
		testDisability(LookupValidationRule.class);
	}
	@Test
	public void testCommon() {
		testCommon(LookupValidationRule.class);
	}
	
	@Test
	public void testReadyness() {
		DataRecord record1 = RF.addIntegerField(null, "id", 10);
		DataRecord record2 = RF.addIntegerField(null, "id", 10);
		record2 = RF.addIntegerField(record2, "id2", 7);
		DataRecord record3 = RF.addIntegerField(null, "a", 10);
		record3 = RF.addIntegerField(record3, "b", 7);
		
		DummyGraphWrapper graphWrapper = new DummyGraphWrapper();
		graphWrapper.addLookupTable(new SimpleLookupTable("testLookup1", record1.getMetadata().getName(), new String[]{"id"}, 0));
		graphWrapper.addLookupTable(new SimpleLookupTable("testLookup2", record2.getMetadata().getName(), new String[]{"id", "id2"}, 0));
		
		assertReadyness(false, newRule("id", "", "id=id", false), record1.getMetadata(), null, graphWrapper);	// Empty lookup name
		assertReadyness(false, newRule("id", "testTable", "id=id", false), record1.getMetadata(), null, graphWrapper);	// Not-existent lookup
		assertReadyness(false, newRule("id", "testLookup1", "id=id2", false), record1.getMetadata(), null, graphWrapper);	// Wrong mapping
		assertReadyness(false, newRule("id", "testLookup1", "", false), record1.getMetadata(), null, graphWrapper);	// Empty mapping	
		assertReadyness(true, newRule("id", "testLookup1", "id=id", false), record1.getMetadata(), null, graphWrapper);
		
		assertReadyness(false, newRule("a,b", "testLookup2", "id=a", false), record3.getMetadata(), null, graphWrapper);
		assertReadyness(false, newRule("a,b", "testLookup2", "id=asdasd", false), record3.getMetadata(), null, graphWrapper);
		assertReadyness(false, newRule("a,b", "testLookup2", "id=b,id2=dd", false), record3.getMetadata(), null, graphWrapper);
		assertReadyness(true, newRule("a,b", "testLookup2", "id=a,id2=b", false), record3.getMetadata(), null, graphWrapper);
		assertReadyness(true, newRule("a,b", "testLookup2", "id2=a,id=b", false), record3.getMetadata(), null, graphWrapper);
	}
	
	@Test
	public void testCorrectSimpleKey() {
		DataRecord record1 = RF.addIntegerField(null, "id", 10);
		
		TransformationGraph graph = new TransformationGraph();
		graph.addDataRecordMetadata(record1.getMetadata());
		
		DummyGraphWrapper graphWrapper = new DummyGraphWrapper();
		graphWrapper.addLookupTable(new SimpleLookupTable("testLookup1", record1.getMetadata().getName(), new String[]{"id"}, 0));
		try {
			graphWrapper.getLookupTable("testLookup1").setGraph(graph);
			graphWrapper.getLookupTable("testLookup1").init();
		} catch (ComponentNotReadyException e) {
			fail("Cannot init lookup table");
		}
		graphWrapper.getLookupTable("testLookup1").put(RF.addIntegerField(null, "id", 8));
		graphWrapper.getLookupTable("testLookup1").put(RF.addIntegerField(null, "id", 6));
		graphWrapper.getLookupTable("testLookup1").put(RF.addIntegerField(null, "id", 4));
		graphWrapper.getLookupTable("testLookup1").put(RF.addIntegerField(null, "id", 2));
		
		assertValid(newRule("a", "testLookup1", "id=a", false), RF.addIntegerField(null, "a", 8), null, graphWrapper);
		assertInvalid(newRule("a", "testLookup1", "id=a", false), RF.addIntegerField(null, "a", 7), null, graphWrapper);
		assertValid(newRule("a", "testLookup1", "id=a", false), RF.addIntegerField(null, "a", 6), null, graphWrapper);
		assertInvalid(newRule("a", "testLookup1", "id=a", false), RF.addIntegerField(null, "a", 5), null, graphWrapper);
		assertValid(newRule("a", "testLookup1", "id=a", false), RF.addIntegerField(null, "a", 4), null, graphWrapper);
		assertInvalid(newRule("a", "testLookup1", "id=a", false), RF.addIntegerField(null, "a", 3), null, graphWrapper);
		assertValid(newRule("a", "testLookup1", "id=a", false), RF.addIntegerField(null, "a", 2), null, graphWrapper);
		
		assertInvalid(newRule("a", "testLookup1", "id=a", true), RF.addIntegerField(null, "a", 8), null, graphWrapper);
		assertValid(newRule("a", "testLookup1", "id=a", true), RF.addIntegerField(null, "a", 7), null, graphWrapper);
		assertInvalid(newRule("a", "testLookup1", "id=a", true), RF.addIntegerField(null, "a", 6), null, graphWrapper);
		assertValid(newRule("a", "testLookup1", "id=a", true), RF.addIntegerField(null, "a", 5), null, graphWrapper);
		assertInvalid(newRule("a", "testLookup1", "id=a", true), RF.addIntegerField(null, "a", 4), null, graphWrapper);
		assertValid(newRule("a", "testLookup1", "id=a", true), RF.addIntegerField(null, "a", 3), null, graphWrapper);
		assertInvalid(newRule("a", "testLookup1", "id=a", true), RF.addIntegerField(null, "a", 2), null, graphWrapper);
	}
	
	@Test
	public void testCorrectCompoundKey() {
		DataRecord record1 = RF.addIntegerField(null, "id", 10);
		record1 = RF.addIntegerField(record1, "id2", 10);
		
		TransformationGraph graph = new TransformationGraph();
		graph.addDataRecordMetadata(record1.getMetadata());
		
		DummyGraphWrapper graphWrapper = new DummyGraphWrapper();
		graphWrapper.addLookupTable(new SimpleLookupTable("testLookup1", record1.getMetadata().getName(), new String[]{"id","id2"}, 0));
		try {
			graphWrapper.getLookupTable("testLookup1").setGraph(graph);
			graphWrapper.getLookupTable("testLookup1").init();
		} catch (ComponentNotReadyException e) {
			fail("Cannot init lookup table");
		}
		graphWrapper.getLookupTable("testLookup1").put(RF.addIntegerField(RF.addIntegerField(null, "id2", 6), "id", 8));
		graphWrapper.getLookupTable("testLookup1").put(RF.addIntegerField(RF.addIntegerField(null, "id2", 8), "id", 6));
		
		assertValid(newRule("a,b", "testLookup1", "id=a,id2=b", false), RF.addIntegerField(RF.addIntegerField(null, "b", 6), "a", 8), null, graphWrapper);
		assertValid(newRule("a,b", "testLookup1", "id=a,id2=b", false), RF.addIntegerField(RF.addIntegerField(null, "b", 8), "a", 6), null, graphWrapper);
		assertInvalid(newRule("a,b", "testLookup1", "id=a,id2=b", false), RF.addIntegerField(RF.addIntegerField(null, "b", 7), "a", 8), null, graphWrapper);
		
		assertInvalid(newRule("a,b", "testLookup1", "id=a,id2=b", true), RF.addIntegerField(RF.addIntegerField(null, "b", 6), "a", 8), null, graphWrapper);
		assertInvalid(newRule("a,b", "testLookup1", "id=a,id2=b", true), RF.addIntegerField(RF.addIntegerField(null, "b", 8), "a", 6), null, graphWrapper);
		assertValid(newRule("a,b", "testLookup1", "id=a,id2=b", true), RF.addIntegerField(RF.addIntegerField(null, "b", 7), "a", 8), null, graphWrapper);

		
	}
	
	private LookupValidationRule newRule(String target, String lookup, String keyMapping, boolean rejectPresent) {
		LookupValidationRule temp = createRule(LookupValidationRule.class);
		temp.getTarget().setValue(target);
		temp.getLookup().setValue(lookup);
		temp.getKeyMapping().setValue(keyMapping);
		if(rejectPresent) {
			temp.getPolicy().setValue(POLICY.REJECT_PRESENT);
		}
		return temp;
	}
}

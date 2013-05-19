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

import org.jetel.component.validator.common.ConversionTestCase;
import org.jetel.data.DataRecord;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.12.2012
 */
public class EnumMatchValidationRuleTest extends ConversionTestCase {
	
	@Test
	public void testNameability() {
		testNameability(EnumMatchValidationRule.class);
	}
	
	@Test
	public void testDisablity() {
		testDisability(EnumMatchValidationRule.class);
	}
	
	@Test
	public void testCommon() {
		testCommon(EnumMatchValidationRule.class);
	}
	
	@Test
	public void testReadyness() {
		DataRecord record = RF.addStringField(null, "field", "");
		assertReadyness(false, newRule("", "first", false, false), record.getMetadata());	// Empty target
		assertReadyness(false, newRule("field2", "first", false, false), record.getMetadata());	// Non-existent target
		assertReadyness(true, newRule("field", "first", false, false), record.getMetadata());
		
		assertReadyness(false, newRule("field", "", false, false), record.getMetadata());	// Empty values
		
		assertReadyness(false, inType("da", df("wrong", newRule("field", "first", false, false))), record.getMetadata());
		assertReadyness(true, inType("da", df("yyyy-MM-dd", newRule("field", "first", false, false))), record.getMetadata());
		assertReadyness(true, inType("da", df("joda:yyyy-MM-dd", newRule("field", "first", false, false))), record.getMetadata());
		assertReadyness(true, inType("da", df("java:yyyy-MM-dd", newRule("field", "first", false, false))), record.getMetadata());
		
		ConversionValidationRule temp;
		temp = inType("da", newRule("field", "first", false, false));
		temp.getParentLanguageSetting().getDateFormat().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("da", newRule("field", "first", false, false));
		temp.getParentLanguageSetting().getLocale().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("da", newRule("field", "first", false, false));
		temp.getParentLanguageSetting().getTimezone().setValue("");
		assertReadyness(false, temp, record.getMetadata());
		
		temp = inType("d", newRule("field", "first", false, false));
		temp.getParentLanguageSetting().getLocale().setValue("");
		assertReadyness(false, temp, record.getMetadata());
	}
	
	@Test
	public void testNormal() {		
		EnumMatchValidationRule rule = newRule("field", "first,first,second,THIRD,fourth", false, false);
		assertValid(rule, RF.addStringField(null, "field", "first"));
		assertValid(rule, RF.addStringField(null, "field", "second"));
		assertValid(rule, RF.addStringField(null, "field", "THIRD"));
		assertValid(rule, RF.addStringField(null, "field", "fourth"));
		
		assertInvalid(rule, RF.addStringField(null, "field", "five"));
		assertInvalid(rule, RF.addStringField(null, "field", "FIRST"));
		assertInvalid(rule, RF.addStringField(null, "field", "SeCoNd"));
		assertInvalid(rule, RF.addStringField(null, "field", "Fourth"));
		assertInvalid(rule, RF.addStringField(null, "field", "third"));
	}
	
	public void testNumbers() {
		ConversionValidationRule rule = inType("l", newRule("field", "10,20,30,40", false, false));
		assertValid(rule, RF.addStringField(null, "field", "10"));
		assertValid(rule, RF.addStringField(null, "field", "20"));
		assertValid(rule, RF.addStringField(null, "field", "30"));
		assertValid(rule, RF.addStringField(null, "field", "40"));
		
		assertInvalid(rule, RF.addStringField(null, "field", "15"));
		assertInvalid(rule, RF.addStringField(null, "field", "-10"));
		assertInvalid(rule, RF.addStringField(null, "field", "21"));
		assertInvalid(rule, RF.addStringField(null, "field", "20.5"));
		assertInvalid(rule, RF.addStringField(null, "field", "40.0"));
	}
	
	public void testLocale() {
		ConversionValidationRule rule = lo("cs.CZ", inType("n", newRule("field", "\"10.5\",20,\"30.6\",40", false, false)));
		rule.getUseType().setValue(ConversionValidationRule.METADATA_TYPES.NUMBER);
		assertValid(rule, RF.addStringField(null, "field", "10,5"));
		assertValid(rule, RF.addStringField(null, "field", "20"));
		assertValid(rule, RF.addStringField(null, "field", "30,6"));
		assertValid(rule, RF.addStringField(null, "field", "40"));
		
		assertInvalid(rule, RF.addStringField(null, "field", "10.5"));
		assertValid(rule, RF.addStringField(null, "field", "20"));
		assertInvalid(rule, RF.addStringField(null, "field", "30.6"));
		assertValid(rule, RF.addStringField(null, "field", "40"));
		
	}
	@Test
	public void testCaseInsensitive() {
		ConversionValidationRule rule = newRule("field", "first,first,SeCoND,THIRD,fourth", true, false);
		assertValid(rule, RF.addStringField(null, "field", "first"));
		assertValid(rule, RF.addStringField(null, "field", "SeCoND"));
		assertValid(rule, RF.addStringField(null, "field", "THIRD"));
		assertValid(rule, RF.addStringField(null, "field", "fourth"));
		assertValid(rule, RF.addStringField(null, "field", "First"));
		assertValid(rule, RF.addStringField(null, "field", "FIRST"));
		assertValid(rule, RF.addStringField(null, "field", "SeCoNd"));
		assertValid(rule, RF.addStringField(null, "field", "Fourth"));
		assertValid(rule, RF.addStringField(null, "field", "third"));
		assertInvalid(rule, RF.addStringField(null, "field", "Five"));
		assertInvalid(rule, RF.addStringField(null, "field", "five"));
		assertInvalid(rule, RF.addStringField(null, "field", ""));
	}
	@Test
	public void testEmptyOption() {
		ConversionValidationRule rule;
		rule = newRule("field", "first,,first,SeCoND,THIRD,fourth", false, false);
		assertValid(rule, RF.addStringField(null, "field", "first"));
		assertValid(rule, RF.addStringField(null, "field", "SeCoND"));
		assertValid(rule, RF.addStringField(null, "field", ""));
		
		rule = newRule("field", ",first,first,SeCoND,THIRD,fourth", false, false);
		assertValid(rule, RF.addStringField(null, "field", "first"));
		assertValid(rule, RF.addStringField(null, "field", "SeCoND"));
		assertValid(rule, RF.addStringField(null, "field", ""));
		
		rule = newRule("field", "first,first,SeCoND,THIRD,fourth,", false, false);
		assertValid(rule, RF.addStringField(null, "field", ""));
	}
	@Test
	public void testTrimming() {
		ConversionValidationRule rule = newRule("field", "first,first,SeCoND,THIRD,fourth", false, true);
		assertValid(rule, RF.addStringField(null, "field", "  first  "));
		assertValid(rule, RF.addStringField(null, "field", "SeCoND   "));
		assertValid(rule, RF.addStringField(null, "field", "  THIRD"));
		assertValid(rule, RF.addStringField(null, "field", "  fourth  "));
		assertInvalid(rule, RF.addStringField(null, "field", "Five"));
		assertInvalid(rule, RF.addStringField(null, "field", "five"));
	}
	@Test
	public void testEscaping() {
		ConversionValidationRule rule = newRule("field", "third,\"first,second\"", false, false);
		assertValid(rule, RF.addStringField(null, "field", "third"));
		assertValid(rule, RF.addStringField(null, "field", "first,second"));
		assertInvalid(rule, RF.addStringField(null, "field", "second"));
		assertInvalid(rule, RF.addStringField(null, "field", "first"));
	}
	
	@Test
	public void testDates() {
		ConversionValidationRule rule;
		
		rule = df("yyyy-MM-dd", inType("da", newRule("field", "2012-10-10,2012-10-12", false, false)));
		assertValid(rule, RF.addStringField(null, "field", "2012-10-10"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012-10-11"));
		assertValid(rule, RF.addStringField(null, "field", "2012-10-12"));
		
		rule = df("yyyy-MM-dd HH:mm:ss", inType("da", newRule("field", "2012-10-10 00:20:10,2012-10-12 10:20:10", false, false)));
		assertValid(rule, RF.addStringField(null, "field", "2012-10-10 00:20:10"));
		assertInvalid(rule, RF.addStringField(null, "field", "2012-10-11 00:20:10"));
		assertValid(rule, RF.addStringField(null, "field", "2012-10-12 10:20:10"));
		
	}
	
	private EnumMatchValidationRule newRule(String target, String values, boolean ignoreCase, boolean trimInput) {
		EnumMatchValidationRule temp = createRule(EnumMatchValidationRule.class);
		temp.getTarget().setValue(target);
		temp.getValues().setValue(values);
		temp.getIgnoreCase().setValue(ignoreCase);
		temp.getTrimInput().setValue(trimInput);
		return temp;
	}

}

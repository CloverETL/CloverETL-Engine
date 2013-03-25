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
package org.jetel.component.validator.utils;

import javax.xml.bind.JAXBException;

import org.jetel.component.validator.AbstractValidationRule;
import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.common.ValidatorTestCase;
import org.jetel.component.validator.rules.EnumMatchValidationRule;
import org.jetel.component.validator.rules.NonEmptyFieldValidationRule;
import org.jetel.component.validator.rules.NonEmptySubsetValidationRule;
import org.jetel.component.validator.rules.PatternMatchValidationRule;
import org.jetel.component.validator.rules.StringLengthValidationRule;
import org.junit.Test;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 10.12.2012
 */
public class ValidationRulesPersisterTest extends ValidatorTestCase {
	
	@Override
	public void setUp() {
//		initEngine();
	}
	
	public ValidationGroup prepare() {
		/*ValidationGroup root = new ValidationGroup();
		root.setName("Root group");
		root.setEnabled(true);
		RangeCheckValidationRule rule = new RangeCheckValidationRule();
		rule.setEnabled(true);
		rule.setName("Test range check");
		rule.getType().setValue(RangeCheckValidationRule.TYPES.COMPARISON);
		rule.getTarget().setValue("number");
		//rule.getOperator().setValue(RangeCheckValidationRule.OPERATOR_TYPE.NE);
		rule.getOperator().setValue(RangeCheckValidationRule.OPERATOR_TYPE.NE);
		rule.getValue().setValue("10");
		root.addChild(rule);
		return root;*/
		
		ValidationGroup rootGroup = new ValidationGroup();
		ValidationGroup root = rootGroup;
		root.setName("Root group");
		root.setEnabled(true);
		AbstractValidationRule br = new NonEmptyFieldValidationRule();
//		br.setName("Neprazdne pole name");
//		setStringParam(br, "target", "name");
//		br.setEnabled(true);
//		root.addChild(br);
//		ValidationNode test = new ValidationGroup();
//		test.setName("Jméno skupiny");
//		root.addChild(test);
//		br = new NonEmptyFieldValidationRule();
//		br.setName("Prazdne pole pokuta");
//		setBooleanParam(br, "checkForEmptiness", true);
//		setStringParam(br, "target", "pokuta");
//		br.setEnabled(true);
//		root.addChild(br);
//		br = new NonEmptySubsetValidationRule();
//		br.setEnabled(true);
//		br.setName("At least 4 nonempty");
//		setStringParam(br, "target", "id,adresa");
//		setIntegerParam(br, "count", 1);
//		root.addChild(br);
//		br = new PatternMatchValidationRule();
//		br.setEnabled(true);
//		br.setName("Name has to contain letter a");
//		setBooleanParam(br, "ignoreCase", true);
//		setStringParam(br, "target", "name");
//		setStringParam(br, "pattern", "^.*[A].*$");
//		root.addChild(br);
//		br = new StringLengthValidationRule();
//		br.setName("Maximal length is 10");
//		br.setEnabled(true);
//		setStringParam(br, "target", "name");
//		setEnumParam(br, "type", StringLengthValidationRule.TYPES.MAXIMAL);
//		setIntegerParam(br, "to", 20);
//		root.addChild(br);
//		ValidationGroup subgroup = new ValidationGroup();
//		subgroup.setName("Podskupina");
//		subgroup.setEnabled(true);
		StringLengthValidationRule br1 = new StringLengthValidationRule();
		br1.setName("MinimaRl length is 2");
		br1.getTarget().setValue("name");
		br1.getFrom().setValue(10);
		br1.getType().setValue(StringLengthValidationRule.TYPES.MINIMAL);
//		subgroup.setPrelimitaryCondition(br);
//		br = new EnumMatchValidationRule();
//		br.setEnabled(true);
//		br.setName("Is YES or MAYBE");
//		setStringParam(br, "target", "type");
//		setStringParam(br, "values", "YES,MAYBE,\"MAY,BE\"");
//		subgroup.addChild(br);
//		root.addChild(subgroup);
		root.addChild(br);
		return root;
	}
	
	@Test
	public void testTest() {
		
	}
	
	@Test
	public void testGeneratingSchema() throws JAXBException {
		System.out.println("Create schema\n-------------------");
		try {
			String schema = ValidationRulesPersister.generateSchema();
			System.out.println(schema);
		} catch (Exception ex) {
			System.out.println("red");
			ex.printStackTrace();
		}
	}
	
	@Test
	public void testSerialize() {		
//		System.out.println("To XML\n-------------------");
//		String output = ValidationRulesPersister.serialize(prepare());
//		System.out.println(output);
//		System.out.println("From XML\n-------------------");
//		ValidationGroup group = ValidationRulesPersister.deserialize(output);
//		System.out.println("To XML again\n-------------------");
//		String output2 = ValidationRulesPersister.serialize(group);
//		System.out.println(output2);
	}
	@Test
	public void testInvalidEnums() {
		System.out.println("Serializace\n");
		System.out.println("---------------------------\n");
		System.out.println(ValidationRulesPersister.serialize(prepare()));
		System.out.println("Deserializace\n");
		System.out.println("---------------------------\n");
		//ValidationGroup group = ValidationRulesPersister.deserialize("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>	<group laziness=\"true\" conjunction=\"AND\" name=\"Root group\" enabled=\"true\">	    <children>	        <rangeCheck name=\"Test range check\" enabled=\"true\">	            <target>number</target>	            <value>10</value>	            <from></from>	            <to></to>	            <boundaries>CLOSED_CLOSED</boundaries>	            <operator>NE</operator>	            <type>COMPARISON</type>	            <useType>DEFAULT</useType>	        </rangeCheck>	    </children>	</group>");
		ValidationRulesPersister.validate(ValidationRulesPersister.serialize(prepare()));
		ValidationGroup group = ValidationRulesPersister.deserialize(ValidationRulesPersister.serialize(prepare()));
//		RangeCheckValidationRule rule = ((RangeCheckValidationRule) group.getChildren().get(0));
//		System.out.println("Target: " + rule.getTarget().getValue());
//		System.out.println(rule.getOperator().getValue());
		System.out.println("Serializace\n");
		System.out.println("---------------------------\n");
		String output2 = ValidationRulesPersister.serialize(group);
		System.out.println(output2);
	}
}

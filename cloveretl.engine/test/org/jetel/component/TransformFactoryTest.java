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
package org.jetel.component;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6.9.2012
 */
public class TransformFactoryTest extends CloverTestCase {

	private TransformationGraph getGraph() {
		return new TransformationGraph();
	}
	
	private Node getComponent() {
		return new Node("myid", getGraph()) {
			@Override
			public String getType() {
				return null;
			}
			@Override
			protected Result execute() throws Exception {
				return null;
			}
		};
	}
	
	private DataRecordMetadata[] getInMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata("inMetadata");
		metadata.addField(new DataFieldMetadata("inField", DataFieldType.STRING, ";"));
		return new DataRecordMetadata[] { metadata };
	}

	private DataRecordMetadata[] getOutMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata("outMetadata");
		metadata.addField(new DataFieldMetadata("outField", DataFieldType.STRING, ";"));
		return new DataRecordMetadata[] { metadata };
	}

	public void testCreateTransformJavaTransform() {
		TransformFactory<Greeter> transformFactory = TransformFactory.createTransformFactory(Greeter.class);
		transformFactory.setTransform(
				"import org.jetel.component.Greeter;\n" +
				"public class NewGreeter extends Greeter {\n" +
						"public String getGreeting(String message) {\n" +
							"return \"New hello \" + message;\n" +
						"}\n" +
				"}\n");
		transformFactory.setComponent(getComponent());
		
		ConfigurationStatus status = new ConfigurationStatus();
		transformFactory.checkConfig(status);
		assertTrue(status.isEmpty());
		assertTrue(transformFactory.isTransformSpecified());
		
		Greeter greeter = transformFactory.createTransform();
		assertEquals("New hello abc", greeter.getGreeting("abc"));
	}

	public void testCreateTransformJavaTransformClass() {
		TransformFactory<Greeter> transformFactory = TransformFactory.createTransformFactory(Greeter.class);
		transformFactory.setTransformClass("org.jetel.component.Greeter");
		transformFactory.setComponent(getComponent());
		
		ConfigurationStatus status = new ConfigurationStatus();
		transformFactory.checkConfig(status);
		assertTrue(status.isEmpty());
		assertTrue(transformFactory.isTransformSpecified());
		
		Greeter greeter = transformFactory.createTransform();
		assertEquals("Hello abc", greeter.getGreeting("abc"));
	}

	public void testCreateTransformJavaTransformUrl() {
		TransformFactory<Greeter> transformFactory = TransformFactory.createTransformFactory(Greeter.class);
		transformFactory.setTransformUrl("data/MyGreeter.java");
		transformFactory.setComponent(getComponent());
		
		ConfigurationStatus status = new ConfigurationStatus();
		transformFactory.checkConfig(status);
		assertTrue(status.isEmpty());
		assertTrue(transformFactory.isTransformSpecified());
		
		Greeter greeter = transformFactory.createTransform();
		assertEquals("New external hello abc", greeter.getGreeting("abc"));
	}

	public void testCreateTransformCTL1Transform() throws TransformException, ComponentNotReadyException {
		TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
		transformFactory.setTransform(
				"//#CTL1\n" +
				"function transform() {\n" +
						"$0.outField := $0.inField + \"!\";\n" +
						"return OK\n" +
				"}\n");
		transformFactory.setComponent(getComponent());
		transformFactory.setInMetadata(getInMetadata());
		transformFactory.setOutMetadata(getOutMetadata());
		ConfigurationStatus status = new ConfigurationStatus();
		transformFactory.checkConfig(status);
		assertTrue(status.isEmpty());
		assertTrue(transformFactory.isTransformSpecified());
		
		RecordTransform recordTransform = transformFactory.createTransform();
		recordTransform.init(null, getInMetadata(), getOutMetadata());
		DataRecord inRecord = DataRecordFactory.newRecord(getInMetadata()[0]);
		inRecord.init();
		inRecord.getField("inField").setValue("input data string");
		DataRecord outRecord = DataRecordFactory.newRecord(getOutMetadata()[0]);
		outRecord.init();
		
		recordTransform.transform(new DataRecord[] { inRecord }, new DataRecord[] { outRecord });
		assertEquals("input data string!", outRecord.getField("outField").getValue().toString());
	}

	public void testCreateTransformCTL1TransformUrl() throws TransformException, ComponentNotReadyException {
		TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
		transformFactory.setTransformUrl("data/TransformFactoryTest.ctl1");
		transformFactory.setComponent(getComponent());
		transformFactory.setInMetadata(getInMetadata());
		transformFactory.setOutMetadata(getOutMetadata());
		ConfigurationStatus status = new ConfigurationStatus();
		transformFactory.checkConfig(status);
		assertTrue(status.isEmpty());
		assertTrue(transformFactory.isTransformSpecified());
		
		RecordTransform recordTransform = transformFactory.createTransform();
		recordTransform.init(null, getInMetadata(), getOutMetadata());
		DataRecord inRecord = DataRecordFactory.newRecord(getInMetadata()[0]);
		inRecord.init();
		inRecord.getField("inField").setValue("input data string");
		DataRecord outRecord = DataRecordFactory.newRecord(getOutMetadata()[0]);
		outRecord.init();
		
		recordTransform.transform(new DataRecord[] { inRecord }, new DataRecord[] { outRecord });
		assertEquals("input data string CTL1 external", outRecord.getField("outField").getValue().toString());
	}

	public void testCreateTransformCTL2Transform() throws TransformException, ComponentNotReadyException {
		TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
		transformFactory.setTransform(
				"//#CTL2\n" +
				"function integer transform() {\n" +
						"$out.0.outField = $in.0.inField + \"!\";\n" +
						"return OK;\n" +
				"}\n");
		transformFactory.setComponent(getComponent());
		transformFactory.setInMetadata(getInMetadata());
		transformFactory.setOutMetadata(getOutMetadata());
		ConfigurationStatus status = new ConfigurationStatus();
		transformFactory.checkConfig(status);
		assertTrue(status.isEmpty());
		assertTrue(transformFactory.isTransformSpecified());
		
		RecordTransform recordTransform = transformFactory.createTransform();
		recordTransform.init(null, getInMetadata(), getOutMetadata());
		DataRecord inRecord = DataRecordFactory.newRecord(getInMetadata()[0]);
		inRecord.init();
		inRecord.getField("inField").setValue("input data string");
		DataRecord outRecord = DataRecordFactory.newRecord(getOutMetadata()[0]);
		outRecord.init();
		
		recordTransform.transform(new DataRecord[] { inRecord }, new DataRecord[] { outRecord });
		assertEquals("input data string!", outRecord.getField("outField").getValue().toString());
	}

	public void testCreateTransformCTL2TransformUrl() throws TransformException, ComponentNotReadyException {
		TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
		transformFactory.setTransformUrl("data/TransformFactoryTest.ctl2");
		transformFactory.setComponent(getComponent());
		transformFactory.setInMetadata(getInMetadata());
		transformFactory.setOutMetadata(getOutMetadata());
		ConfigurationStatus status = new ConfigurationStatus();
		transformFactory.checkConfig(status);
		assertTrue(status.isEmpty());
		assertTrue(transformFactory.isTransformSpecified());
		
		RecordTransform recordTransform = transformFactory.createTransform();
		recordTransform.init(null, getInMetadata(), getOutMetadata());
		DataRecord inRecord = DataRecordFactory.newRecord(getInMetadata()[0]);
		inRecord.init();
		inRecord.getField("inField").setValue("input data string");
		DataRecord outRecord = DataRecordFactory.newRecord(getOutMetadata()[0]);
		outRecord.init();
		
		recordTransform.transform(new DataRecord[] { inRecord }, new DataRecord[] { outRecord });
		assertEquals("input data string CTL2 external", outRecord.getField("outField").getValue().toString());
	}

	public void testCheckConfig() {
		TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
		ConfigurationStatus status = new ConfigurationStatus();
		
		transformFactory.checkConfig(status);
		assertTrue(status.isError());
		assertFalse(transformFactory.isTransformSpecified());

		transformFactory.setComponent(getComponent());
		transformFactory.checkConfig(status);
		assertTrue(status.isError());
		assertFalse(transformFactory.isTransformSpecified());

		transformFactory.setTransform("class Neco {}");
		transformFactory.checkConfig(status);
		assertTrue(transformFactory.isTransformSpecified());
	}

	public void testDynamicCompilerOff() {
		boolean oldUseDynamicCompiler = Defaults.USE_DYNAMIC_COMPILER;
		try {
			Defaults.USE_DYNAMIC_COMPILER = false;
			
			//following dynamic compilation should not be possible without dynamic compiler
			try {
				TransformFactory<Greeter> transformFactory = TransformFactory.createTransformFactory(Greeter.class);
				transformFactory.setTransform(
						"import org.jetel.component.Greeter;\n" +
						"public class NewGreeter extends Greeter {\n" +
								"public String getGreeting(String message) {\n" +
									"return \"New hello \" + message;\n" +
								"}\n" +
						"}\n");
				transformFactory.setComponent(getComponent());
				transformFactory.createTransform();
				assertTrue(false);
			} catch (Exception e) {
				//OK
			}
			
			//COMPILED mode of CTL2 is not available without dynamic compiler - regular interpreted mode should be used instead
			TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
			transformFactory.setTransform(
					"//#CTL2:COMPILED\n" +
					"function integer transform() {\n" +
							"$out.0.outField = $in.0.inField + \"!\";\n" +
							"return OK;\n" +
					"}\n");
			transformFactory.setComponent(getComponent());
			transformFactory.setInMetadata(getInMetadata());
			transformFactory.setOutMetadata(getOutMetadata());
			RecordTransform recordTransform = transformFactory.createTransform();
			assertTrue(recordTransform instanceof CTLRecordTransformAdapter);
		} finally {
			Defaults.USE_DYNAMIC_COMPILER = oldUseDynamicCompiler;
		}
	}
	
}

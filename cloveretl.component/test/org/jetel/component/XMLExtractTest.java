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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Edge;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1 Jul 2011
 */
public class XMLExtractTest extends CloverTestCase {

	private static final String TEST_RESOURCERS_DIR = "test-data/"; // path relative to cloveretl.component project dir

	private static String[][] NO_RECORDS = new String[0][0];
	
	private DataRecordMetadata metadata1;
	
	public XMLExtractTest(String name) {
		super(name);
		initEngine();
		createMetadata();

		System.out.println("\nTest resources directory set to " + new File(TEST_RESOURCERS_DIR).getAbsolutePath());
	}

	private void createMetadata() {
		metadata1 = new DataRecordMetadata("md1", DataRecordMetadata.DELIMITED_RECORD);
		metadata1.addField(new DataFieldMetadata("f1", DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("f2", DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("f3", DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("f4", DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("f5", DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("f6", DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("f7", DataFieldMetadata.STRING_FIELD, ";"));
	}
	

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		System.out.println("------");
	}

	@Override
	protected void tearDown() throws Exception {
	}

	public void testUseNestedNodes1T() throws Exception {
		XMLExtract xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "UNN1.xml", true, metadata1, metadata1);

		//printEdgeRecords(xmlExtract, 0);
		checkOutPortRecords(xmlExtract, 0,
				rec("fooVal1"),
				rec(""),
				rec("fooVal3"),
				rec("fooVal4"),
				rec("fooVal5")
				);
		//printEdgeRecords(xmlExtract, 1);
		checkOutPortRecords(xmlExtract, 1,
				rec("e1AttrFoo"),
				rec("eAttrFoo2")
				);
	}
	
	public void testUseNestedNodes1F() throws Exception {
		XMLExtract xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "UNN1.xml", false, metadata1, metadata1);
		//printEdgeRecords(xmlExtract, 0);
		checkOutPortRecords(xmlExtract, 0,
				rec("fooVal3"),
				rec("fooVal4")
				);
		//printEdgeRecords(xmlExtract, 1);
		checkOutPortRecords(xmlExtract, 1,
				rec(),
				rec("eAttrFoo1")
				);

		xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "UNN1-1.xml", false, metadata1, metadata1);
		checkOutPortRecords(xmlExtract, 0, NO_RECORDS);
		checkOutPortRecords(xmlExtract, 1,
				rec("fooVal3"),
				rec("eAttrFoo1")
				);
	}

	public void testUseNestedNodes2T() throws Exception {
		XMLExtract xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "UNN2.xml", true, metadata1);
		checkOutPortRecords(xmlExtract, 0,
				rec("fooVal1"),
				rec(""),
				rec("fooVal5")
				);
	}
	
	public void testUseNestedNodes2F() throws Exception {
		XMLExtract xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "UNN2.xml", false, metadata1);
		checkOutPortRecords(xmlExtract, 0,
				rec("fooVal1"),
				rec(""),
				rec("fooVal5")
				);
	}
	
	public void testAncestorReference1() throws Exception {
		XMLExtract xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "AR1.xml", false, metadata1, metadata1, metadata1);
		checkOutPortRecords(xmlExtract, 0,
				rec("a1"),
				rec("a9")
				);
		checkOutPortRecords(xmlExtract, 1,
				rec("a2", "a1", null, "b1Val", "c1Val"),
				rec("a3", "a1"),
				rec("a4", "a1")
				);
		checkOutPortRecords(xmlExtract, 2,
				rec("a5", "a2", "a1", null, null, null, "c1Val"),
				rec("a6", "a2", "a1", null, "b1Val", null, "c1Val"),
				rec("a7", "a4", "a1")
				);
		
		xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "AR1.xml", true, metadata1, metadata1, metadata1);
		checkOutPortRecords(xmlExtract, 0,
				rec("a1"),
				rec("a9")
				);
		checkOutPortRecords(xmlExtract, 1,
				rec("a2", "a1", null, "b1Val", "c1Val"),
				rec("a3", "a1"),
				rec("a4", "a1")
				);
		checkOutPortRecords(xmlExtract, 2,
				rec("a5", "a2", "a1", null, null, null, "c1Val"),
				rec("a8", "a2", "a1", "b2Val", "b1Val", null, "c1Val"),
				rec("a7", "a4", "a1")
				);
	}

	public void testAncestorReference2() throws Exception {
		XMLExtract xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "AR2.xml", false, metadata1);
		checkOutPortRecords(xmlExtract, 0,
				rec("b1Val", "a2", "a1"),
				rec("b2Val", "a8", "a6", "a2", "a6Val", null, "c1Val")
				);

		xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "AR2.xml", true, metadata1);
		checkOutPortRecords(xmlExtract, 0,
				rec("b1Val", "a2", "a1"),
				rec("b2Val", "a8", "a6", "a2", "a6Val", null, "c1Val")
				);
	}
	
	public void testTemplate1() throws Exception {
		XMLExtract xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "T1.xml", false, metadata1);
		//printEdgeRecords(xmlExtract, 0);
		checkOutPortRecords(xmlExtract, 0,
				rec("a5", "a2", null, "c1Val"),
				rec("a8", "a6", "b2Val", null, "a6Val"),
				rec("a6", "a2", null, "c1Val"),
				rec("a2", "a1", "b1Val"),
				rec("a3", "a1"),
				rec("a7", "a4"),
				rec("a4", "a1"),
				rec("a1"),
				rec("a9")
				);

		xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "T1.xml", true, metadata1);
		//printEdgeRecords(xmlExtract, 0);
		checkOutPortRecords(xmlExtract, 0,
				rec("a5", "a2", null, "c1Val"),
				rec("a8", "a6", "b2Val", null, "a6Val"),
				rec("a6", "a2", null, "c1Val"),
				rec("a2", "a1", "b1Val"),
				rec("a3", "a1"),
				rec("a7", "a4"),
				rec("a4", "a1"),
				rec("a1"),
				rec("a9")
				);
	}
	
	public void testTemplate2() throws Exception {
		XMLExtract xmlExtract = createAndRunXMLExtract("XMLExtractTest.xml", "T2.xml", false, metadata1);
		//printEdgeRecords(xmlExtract, 0);
		checkOutPortRecords(xmlExtract, 0,
				rec("a5", "a2"),
				rec("a8", "a6"),
				rec("a6", "a2"),
				rec("a2", "a1"),
				rec("a3", "a1"),
				rec("a7", "a4"),
				rec("a4", "a1"),
				rec("a1"),
				rec("a9")
				);
	}
	
	
	private static void checkOutPortRecords(XMLExtract xmlExtract, int outPort, String[]... expectedRecords)
	throws IOException, InterruptedException {
		Edge edge = (Edge) xmlExtract.getOutputPort(outPort);
		assertNotNull("No edge connected to out port " + outPort, edge);
		assertEquals("Number of buffered out port " + outPort + " records ", expectedRecords.length, edge.getBufferedRecords());
		
		DataRecord actualRecord = DataRecordFactory.newRecord(edge.getMetadata());
		DataRecord expectedRecord = DataRecordFactory.newRecord(edge.getMetadata());
		actualRecord.init();
		expectedRecord.init();
		expectedRecord.setToNull(); // fields excluded from expectedRecords[i] are considered to be nulls

		int recordNum = 0;
		while (edge.readRecord(actualRecord) != null) {
			assertTrue("Error in test: expected record #"+recordNum+" has more fields than the actual one;\n" +
					"Expected record:\n" + Arrays.toString(expectedRecords[recordNum]) +
					"Actual record:\n" + actualRecord, expectedRecords[recordNum].length <= actualRecord.getNumFields());
			// fill expectedRecord from expectedRecords[recordNum]
			int i = 0;
			for (; i < expectedRecords[recordNum].length; i++) {
				String value = expectedRecords[recordNum][i];
				expectedRecord.getField(i).fromString(value);
				if (value != null && expectedRecord.getField(i).isNull()) {
					// try to fix incorrect null caused by fromString() with combination of metadata null value
					expectedRecord.getField(i).setValue(value);
				}
			}
			for (; i < expectedRecord.getNumFields(); i++) {
				expectedRecord.getField(i).setNull(true);
			}
			
			// compare expected and actual record
			for (i = 0; i < actualRecord.getNumFields(); i++) {
				DataField actualField = actualRecord.getField(i);
				DataField expectedField = expectedRecord.getField(i);
				if (actualField.isNull() && expectedField.isNull()) continue;
				assertTrue("Unexpected record #" + recordNum + " in out port " + outPort +
						"\nUnexpected field " + i +"; expected <" + expectedField.getValue() + "> but was <"+ actualField + ">\n" +
						"\nExpected record:\n" + expectedRecord + "\nActual record:\n" + actualRecord, actualField.equals(expectedField));
			}
			
			actualRecord.setToNull();
			recordNum++;
		}
	}
	
	/** Mmmmm, syntactic sugar! Sweet :-) */
	private static String[] rec(String... fields) {
		return fields;
	}

	private void printEdgeRecords(Edge edge) throws IOException, InterruptedException {
		DataRecord record = DataRecordFactory.newRecord(edge.getMetadata());
		record.init();
		while (edge.readRecord(record) != null) {
			System.out.println(record);
		}
	}

	private void printEdgeRecords(XMLExtract xmlExtract, int outPort) throws IOException, InterruptedException {
		printEdgeRecords((Edge)xmlExtract.getOutputPort(outPort));
	}
	
	private XMLExtract createAndRunXMLExtract(String inFile, String mappingFile, boolean useNestedNodes, DataRecordMetadata... edgesMetadata) throws Exception, ComponentNotReadyException {
		XMLExtract xmlExtract = createXMLExtract(inFile, mappingFile, useNestedNodes, edgesMetadata);

		xmlExtract.preExecute();
		xmlExtract.init();

		assertEquals("XMLExtract execution failed!", Result.FINISHED_OK, xmlExtract.execute());
		return xmlExtract;
	}

	private static XMLExtract createXMLExtract(String inFile, String mappingFile, boolean useNestedNodes, DataRecordMetadata... edgesMetadata)
	throws Exception {
		XMLExtract xmlExtract = new XMLExtract("TestXMLExtract");
		xmlExtract.setInputFile(TEST_RESOURCERS_DIR + inFile);
		xmlExtract.setMappingURL(TEST_RESOURCERS_DIR + mappingFile);
		xmlExtract.setUseNestedNodes(useNestedNodes);

		xmlExtract.setGraph(new TransformationGraph("TestXMLExtractGraph"));
		
		for (int i = 0; i < edgesMetadata.length; i++) {
			Edge edge = new Edge("Edge" + i, edgesMetadata[i]);
			edge.setEdgeType(EdgeTypeEnum.BUFFERED);
			edge.init();
			xmlExtract.addOutputPort(i, edge);
		}
		
		return xmlExtract;
	}
	
}

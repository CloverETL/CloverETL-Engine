/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6 Mar 2012
 */
public class TrackingMetadataToolkitTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}

	public void testCreateMetadata() throws XMLConfigurationException, GraphConfigurationException, IOException {
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("./test-data/TrackingMetadataToolkitTest.grf"),
				new GraphRuntimeContext());
		DataRecordMetadata metadata = TrackingMetadataToolkit.createMetadata(graph);
		for (Entry<String, String> fieldEntry : getMetadataConcept().entrySet()) {
			assertTrue(metadata.getField(fieldEntry.getKey()) != null);
			assertTrue(metadata.getField(fieldEntry.getKey()).getDataType().getName().equals(fieldEntry.getValue()));
		}
	}
	
	private Map<String, String> getMetadataConcept() throws IOException {
		Map<String, String> result = new HashMap<String, String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("MetadataConcept.txt")));
		String line;
		while ((line = br.readLine()) != null) {
			result.put(line.substring(0, line.indexOf(' ')), line.substring(line.indexOf(' ') + 1, line.length()));
		}
		return result;
	}
	
}

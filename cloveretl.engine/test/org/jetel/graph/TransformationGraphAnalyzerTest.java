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
package org.jetel.graph;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31. 10. 2013
 */
public class TransformationGraphAnalyzerTest extends CloverTestCase {

	public void testNodesTopologicalSorting_01() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_01.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR", "SIMPLE_COPY2", "EXT_FILTER1", "SIMPLE_GATHER", "EXT_SORT1");
		
		sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(1).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR1", "REFORMAT", "EXT_SORT2", "EXT_MERGE_JOIN", "EXT_FILTER", "EXT_SORT", "TRASH", "SIMPLE_COPY", "TRASH1", "TRASH2", "TRASH3");
	}

	public void testNodesTopologicalSorting_02() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_02.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR", "SIMPLE_COPY", "TRASH");
		
		sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(1).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR1", "SIMPLE_COPY1", "TRASH1");
	}

	public void testNodesTopologicalSorting_03() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_03.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR", "SIMPLE_COPY", "TRASH");
		
		sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(1).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR1", "SIMPLE_GATHER", "TRASH1");
	}

	public void testNodesTopologicalSorting_04() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_04.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR1", "DATA_GENERATOR", "SIMPLE_COPY", "SIMPLE_GATHER", "TRASH");
		
		sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(1).getNodes().values()));
		checkSortedNodes(sortedNodes, "TRASH1");
	}

	public void testNodesTopologicalSorting_05() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_05.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR1", "DATA_GENERATOR", "SIMPLE_COPY", "SIMPLE_GATHER", "TRASH");
	}

	public void testNodesTopologicalSorting_06() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_06.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR", "LOOP", "SIMPLE_COPY", "TRASH");
	}

	public void testNodesTopologicalSorting_07() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_07.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "LOOP", "SIMPLE_COPY");
	}

	public void testNodesTopologicalSorting_08() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_08.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR", "LOOP", "SIMPLE_COPY", "SIMPLE_COPY1", "TRASH");
	}

	public void testNodesTopologicalSorting_09() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/TopologicalSorting_09.grf"), runtimeContext);
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR", "LOOP1", "SIMPLE_COPY2", "SIMPLE_COPY1", "LOOP", "SIMPLE_COPY", "TRASH");
	}

	public void testNodesTopologicalSorting_10() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraphXMLReaderWriter graphReaderWriter = new TransformationGraphXMLReaderWriter(runtimeContext);
		graphReaderWriter.setStrictParsing(false);
		TransformationGraph graph = graphReaderWriter.read(new FileInputStream("data/graph/TopologicalSorting_10.grf"));
		
		List<Node> sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(0).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR1", "DATA_GENERATOR", "LOOP1", "SIMPLE_COPY2", "SIMPLE_COPY1", "SIMPLE_COPY6", "SIMPLE_GATHER2", "LOOP", "SIMPLE_COPY", "TRASH", "SIMPLE_COPY3", "SIMPLE_GATHER", "TRASH1");

		sortedNodes = TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(graph.getPhase(1).getNodes().values()));
		checkSortedNodes(sortedNodes, "DATA_GENERATOR2", "SIMPLE_COPY4", "SIMPLE_COPY5", "SIMPLE_GATHER1", "TRASH2");
	}

	private void checkSortedNodes(List<Node> sortedNodes, String... expectedNodeIds) {
		List<String> nodeIds = expectedNodeIds != null ? Arrays.asList(expectedNodeIds) : new ArrayList<String>();
		int i = 0;
		for (Node node : sortedNodes) {
			assertEquals(nodeIds.get(i++), node.getId());
		}
	}
	
}

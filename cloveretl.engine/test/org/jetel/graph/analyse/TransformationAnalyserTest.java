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
package org.jetel.graph.analyse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;

import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.1.2013
 */
public class TransformationAnalyserTest  extends CloverTestCase {

	@Override
	protected void setUp() { 
		initEngine();
	}

	public void testAnalyseGraph_01() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/GraphAnalyse_01.grf"), runtimeContext);
		TransformationAnalyser.analyseGraph(graph);
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge1").getEdgeType());
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge2").getEdgeType());
		assertEquals(EdgeTypeEnum.DIRECT, graph.getEdges().get("Edge0").getEdgeType());
		assertEquals(EdgeTypeEnum.DIRECT, graph.getEdges().get("Edge3").getEdgeType());
	}

	public void testAnalyseGraph_02() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/GraphAnalyse_02.grf"), runtimeContext);
		TransformationAnalyser.analyseGraph(graph);
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge0").getEdgeType());
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge3").getEdgeType());
	}

	public void testAnalyseGraph_03() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/GraphAnalyse_03.grf"), runtimeContext);
		TransformationAnalyser.analyseGraph(graph);
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge1").getEdgeType());
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge2").getEdgeType());
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge4").getEdgeType());
		assertEquals(EdgeTypeEnum.DIRECT, graph.getEdges().get("Edge0").getEdgeType());
		assertEquals(EdgeTypeEnum.DIRECT, graph.getEdges().get("Edge3").getEdgeType());
	}

	public void testAnalyseGraph_04() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/GraphAnalyse_04.grf"), runtimeContext);
		TransformationAnalyser.analyseGraph(graph);
		assertEquals(EdgeTypeEnum.PHASE_CONNECTION, graph.getEdges().get("Edge1").getEdgeType());
		assertEquals(EdgeTypeEnum.PHASE_CONNECTION, graph.getEdges().get("Edge2").getEdgeType());
		assertEquals(EdgeTypeEnum.PHASE_CONNECTION, graph.getEdges().get("Edge4").getEdgeType());
		assertEquals(EdgeTypeEnum.DIRECT, graph.getEdges().get("Edge0").getEdgeType());
		assertEquals(EdgeTypeEnum.DIRECT, graph.getEdges().get("Edge3").getEdgeType());
	}

	public void testAnalyseGraph_05() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/GraphAnalyse_05.grf"), runtimeContext);
		TransformationAnalyser.analyseGraph(graph);
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge6").getEdgeType());
		assertEquals(EdgeTypeEnum.BUFFERED, graph.getEdges().get("Edge7").getEdgeType());
		assertEquals(EdgeTypeEnum.DIRECT, graph.getEdges().get("Edge4").getEdgeType());
		assertEquals(EdgeTypeEnum.DIRECT, graph.getEdges().get("Edge5").getEdgeType());
	}

	public void testAnalyseGraph_06() throws FileNotFoundException, XMLConfigurationException, GraphConfigurationException, MalformedURLException {
		GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
		runtimeContext.setContextURL(new File("data").toURI().toURL());
		TransformationGraph graph = TransformationGraphXMLReaderWriter.loadGraph(new FileInputStream("data/graph/GraphAnalyse_06.grf"), runtimeContext);
		try {
			TransformationAnalyser.analyseGraph(graph);
			assertTrue(false);
		} catch (Exception e) {
			//OK
		}
	}

}
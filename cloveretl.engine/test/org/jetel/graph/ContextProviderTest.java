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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jetel.exception.GraphConfigurationException;
import org.jetel.graph.ContextProvider.Context;
import org.jetel.test.CloverTestCase;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.6.2012
 */
public class ContextProviderTest extends CloverTestCase {

	public void testGetGraph() throws InterruptedException, ExecutionException {
		final TransformationGraph graph1 = new TransformationGraph();
		final TransformationGraph graph2 = new TransformationGraph();
		final TransformationGraph graph3 = new TransformationGraph();
		
		assertNull(ContextProvider.getGraph());
		
		Context c1 = ContextProvider.registerGraph(graph1);
		assertEquals(graph1, ContextProvider.getGraph());
		
		Future<?> future = Executors.newSingleThreadExecutor().submit(new Runnable() {
			@Override
			public void run() {
				assertNull(ContextProvider.getGraph());

				ContextProvider.registerGraph(graph2);
				assertEquals(graph2, ContextProvider.getGraph());

				Context c1 = ContextProvider.registerGraph(graph1);
				assertEquals(graph1, ContextProvider.getGraph());
				
				ContextProvider.unregister(c1);
				assertEquals(graph2, ContextProvider.getGraph());

				ContextProvider.registerGraph(graph3);
				assertEquals(graph3, ContextProvider.getGraph());

				ContextProvider.unregisterAll();
				assertNull(ContextProvider.getGraph());
			}
		});
		
		Context c2 = ContextProvider.registerGraph(graph2);
		assertEquals(graph2, ContextProvider.getGraph());

		Context cNull = ContextProvider.registerGraph(null);
		assertNull(cNull);

		cNull = ContextProvider.registerNode(null);
		assertNull(cNull);

		ContextProvider.unregister(cNull);
		assertEquals(graph2, ContextProvider.getGraph());

		ContextProvider.unregister(c2);
		assertEquals(graph1, ContextProvider.getGraph());

		Context c3_1 = ContextProvider.registerGraph(graph3);
		assertEquals(graph3, ContextProvider.getGraph());

		Context c3_2 = ContextProvider.registerGraph(graph3);
		assertEquals(graph3, ContextProvider.getGraph());

		ContextProvider.unregister(c3_2);
		assertEquals(graph3, ContextProvider.getGraph());

		ContextProvider.unregister(c3_1);
		assertEquals(graph1, ContextProvider.getGraph());

		ContextProvider.unregister(c1);
		assertNull(ContextProvider.getGraph());

		future.get();
	}

	public void testGetNode() throws InterruptedException, ExecutionException, GraphConfigurationException {
		final TransformationGraph graph1 = new TransformationGraph();
		final Node node1 = new DummyNode(graph1);
		final Phase phase1 = new Phase(0);
		graph1.addPhase(phase1);
		phase1.addNode(node1);
		
		final TransformationGraph graph2 = new TransformationGraph();
		final Node node2 = new DummyNode(graph2);
		final Phase phase2 = new Phase(0);
		graph2.addPhase(phase2);
		phase2.addNode(node2);

		final TransformationGraph graph3 = new TransformationGraph();
		final Node node3 = new DummyNode(graph3);
		final Phase phase3 = new Phase(0);
		graph3.addPhase(phase3);
		phase3.addNode(node3);

		assertNull(ContextProvider.getNode());
		
		Context c1 = ContextProvider.registerNode(node1);
		assertEquals(node1, ContextProvider.getNode());
		assertEquals(graph1, ContextProvider.getGraph());
		
		Future<?> future = Executors.newSingleThreadExecutor().submit(new Runnable() {
			@Override
			public void run() {
				assertNull(ContextProvider.getGraph());

				ContextProvider.registerNode(node2);
				assertEquals(node2, ContextProvider.getNode());
				assertEquals(graph2, ContextProvider.getGraph());

				Context c1 = ContextProvider.registerNode(node1);
				assertEquals(node1, ContextProvider.getNode());
				assertEquals(graph1, ContextProvider.getGraph());
				
				ContextProvider.unregister(c1);
				assertEquals(node2, ContextProvider.getNode());
				assertEquals(graph2, ContextProvider.getGraph());

				ContextProvider.registerNode(node3);
				assertEquals(node3, ContextProvider.getNode());
				assertEquals(graph3, ContextProvider.getGraph());

				ContextProvider.unregisterAll();
				assertNull(ContextProvider.getNode());
				assertNull(ContextProvider.getGraph());
			}
		});
		
		Context c2 = ContextProvider.registerNode(node2);
		assertEquals(node2, ContextProvider.getNode());
		assertEquals(graph2, ContextProvider.getGraph());

		ContextProvider.unregister(c2);
		assertEquals(node1, ContextProvider.getNode());
		assertEquals(graph1, ContextProvider.getGraph());

		Context c3_1 = ContextProvider.registerNode(node3);
		assertEquals(node3, ContextProvider.getNode());
		assertEquals(graph3, ContextProvider.getGraph());

		Context c3_2 = ContextProvider.registerNode(node3);
		assertEquals(node3, ContextProvider.getNode());
		assertEquals(graph3, ContextProvider.getGraph());

		ContextProvider.unregister(c3_2);
		assertEquals(node3, ContextProvider.getNode());
		assertEquals(graph3, ContextProvider.getGraph());

		ContextProvider.unregister(c3_1);
		assertEquals(node1, ContextProvider.getNode());
		assertEquals(graph1, ContextProvider.getGraph());

		ContextProvider.unregister(c1);
		assertNull(ContextProvider.getNode());
		assertNull(ContextProvider.getGraph());

		future.get();
	}

	private class DummyNode extends Node {
		public DummyNode(TransformationGraph graph) {
			super("neco", graph);
		}

		@Override
		public String getType() {
			return null;
		}

		@Override
		protected Result execute() throws Exception {
			return null;
		}
	}
	
}

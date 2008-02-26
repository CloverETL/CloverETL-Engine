/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.graph;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.runtime.GraphExecutor;

public class GraphPool {

    private static final Log logger = LogFactory.getLog(GraphPool.class);

	private final String graphSource;
	
	private final Properties graphProperties;
	
	private final List<TransformationGraph> graphs;
	
	private final int size;
	
	private int count;
	
	public GraphPool(int size, String graphSource, Properties graphProperties) {
		this.size = size;
		this.graphSource = graphSource;
		this.graphProperties = graphProperties;
		graphs = new ArrayList<TransformationGraph>();
	}
	
	public synchronized TransformationGraph getGraph() throws XMLConfigurationException, GraphConfigurationException {
		while(size > 0 && count >= size) {
			try {
				wait();
			} catch (InterruptedException e) {
				//DO NOTHING
			}
		}
		count++;
		if(graphs.isEmpty()) {
			return createNewInstance();
		}
		return graphs.remove(graphs.size() - 1);
	}

	public synchronized void releaseGraph(TransformationGraph graph) {
		if(graph == null) {
			throw new NullPointerException();
		}
		graphs.add(graph);
		
		count--;
		notifyAll();
	}
	
	public synchronized void freeGraph(TransformationGraph graph) {
		if(graph == null) {
			throw new NullPointerException();
		}
		
		count--;
		notifyAll();
	}
	
	public synchronized void free() {
		for(TransformationGraph graph : graphs) {
			graph.free();
		}
	}
	
	private TransformationGraph createNewInstance() throws XMLConfigurationException, GraphConfigurationException {
		TransformationGraph graph = null;

		try {
			graph = GraphExecutor.loadGraph(new ByteArrayInputStream(graphSource.getBytes(Defaults.DataParser.DEFAULT_CHARSET_DECODER)), graphProperties);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unexpected exception: ", e);
		}
		
		return graph;
	}
	
}

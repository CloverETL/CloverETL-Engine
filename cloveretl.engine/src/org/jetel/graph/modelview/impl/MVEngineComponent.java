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
package org.jetel.graph.modelview.impl;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.component.MetadataProvider;
import org.jetel.graph.Node;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * General model wrapper for engine component ({@link Node}).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 8. 2013
 */
public class MVEngineComponent implements MVComponent<DataRecordMetadata> {

	private Node engineComponent;
	
	private Map<Integer, MVEdge<DataRecordMetadata>> inputEdges;

	private Map<Integer, MVEdge<DataRecordMetadata>> outputEdges;
	
	public MVEngineComponent(Node engineComponent) {
		this.engineComponent = engineComponent;
		
		inputEdges = new LinkedHashMap<Integer, MVEdge<DataRecordMetadata>>();
		for (Entry<Integer, org.jetel.graph.InputPort> entry : engineComponent.getInputPorts().entrySet()) {
			inputEdges.put(entry.getKey(), new MVEngineEdge(entry.getValue().getEdge()));
		}
		
		outputEdges = new LinkedHashMap<Integer, MVEdge<DataRecordMetadata>>();
		for (Entry<Integer, org.jetel.graph.OutputPort> entry : engineComponent.getOutputPorts().entrySet()) {
			outputEdges.put(entry.getKey(), new MVEngineEdge(entry.getValue().getEdge()));
		}
	}

	@Override
	public Map<Integer, MVEdge<DataRecordMetadata>> getInputEdges() {
		return inputEdges;
	}

	@Override
	public Map<Integer, MVEdge<DataRecordMetadata>> getOutputEdges() {
		return outputEdges;
	}

	@Override
	public boolean isPassThrough() {
		return engineComponent.getDescription().isPassThrough();
	}

	@Override
	public MVMetadata<DataRecordMetadata> getDefaultOutputMetadata(int portIndex) {
		if (engineComponent instanceof MetadataProvider) {
			return new MVEngineMetadata(((MetadataProvider) engineComponent).getOutputMetadata(portIndex));
		} else {
			return null;
		}
	}

	@Override
	public MVMetadata<DataRecordMetadata> getDefaultInputMetadata(int portIndex) {
		if (engineComponent instanceof MetadataProvider) {
			return new MVEngineMetadata(((MetadataProvider) engineComponent).getInputMetadata(portIndex));
		} else {
			return null;
		}
	}
	
	@Override
	public int hashCode() {
		return engineComponent.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MVEngineComponent)) { 
			return false;
		}
		return engineComponent.equals(((MVEngineComponent) obj).engineComponent);
	}
	
}

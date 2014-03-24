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

import org.jetel.component.ComponentMetadataProvider;
import org.jetel.component.MetadataProvider;
import org.jetel.graph.Node;
import org.jetel.graph.modelview.MVComponent;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVGraph;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.MetadataRepository;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.string.StringUtils;

/**
 * General model wrapper for engine component ({@link Node}).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 8. 2013
 */
public class MVEngineComponent implements MVComponent {

	private Node engineComponent;
	
	private Map<Integer, MVEdge> inputEdges;

	private Map<Integer, MVEdge> outputEdges;
	
	private MVGraph parentMVGraph;
	
	MVEngineComponent(Node engineComponent, MVGraph parentMVGraph) {
		if (engineComponent == null || parentMVGraph == null) {
			throw new IllegalArgumentException("MVEngineComponent init failed");
		}
		this.engineComponent = engineComponent;
		this.parentMVGraph = parentMVGraph;

		inputEdges = new LinkedHashMap<Integer, MVEdge>();
		for (Entry<Integer, org.jetel.graph.InputPort> entry : engineComponent.getInputPorts().entrySet()) {
			inputEdges.put(entry.getKey(), parentMVGraph.getMVEdge(entry.getValue().getEdge().getId()));
		}
		
		outputEdges = new LinkedHashMap<Integer, MVEdge>();
		for (Entry<Integer, org.jetel.graph.OutputPort> entry : engineComponent.getOutputPorts().entrySet()) {
			outputEdges.put(entry.getKey(), parentMVGraph.getMVEdge(entry.getValue().getEdge().getId()));
		}
	}

	@Override
	public Node getModel() {
		return engineComponent;
	}
	
	@Override
	public String getId() {
		return engineComponent.getId();
	}
	
	@Override
	public void reset() {
		//DO NOTHING
	}
	
	@Override
	public Map<Integer, MVEdge> getInputEdges() {
		return inputEdges;
	}

	@Override
	public Map<Integer, MVEdge> getOutputEdges() {
		return outputEdges;
	}

	@Override
	public boolean isPassThrough() {
		return engineComponent.getDescriptor().isPassThrough();
	}

	/**
	 * @return metadata provider based on engine component
	 */
	private static MetadataProvider getMetadataProvider(Node engineComponent) {
		MetadataProvider metadataProvider = null;
		if (engineComponent instanceof MetadataProvider) {
			metadataProvider = (MetadataProvider) engineComponent;
		} else if (engineComponent != null && !StringUtils.isEmpty(engineComponent.getDescriptor().getMetadataProvider())) {
			metadataProvider = ClassLoaderUtils.loadClassInstance(MetadataProvider.class, engineComponent.getDescriptor().getMetadataProvider(), engineComponent);
			if (metadataProvider instanceof ComponentMetadataProvider) {
				((ComponentMetadataProvider) metadataProvider).setComponent(engineComponent);
			}
		}
		return metadataProvider;
	}
	
	@Override
	public MVMetadata getDefaultOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		//first let's try to find default metadata dynamically from component instance
		MetadataProvider metadataProvider = getMetadataProvider(engineComponent);
		if (metadataProvider != null) {
			MVMetadata metadata = metadataProvider.getOutputMetadata(portIndex, metadataPropagationResolver);
			if (metadata != null) {
				return metadata;
			}
		}

		//no dynamic metadata found, let's use static metadata from component descriptor 
		String metadataId = engineComponent.getDescriptor().getDefaultOutputMetadataId(portIndex);
		return getStaticMetadata(metadataId);
	}

	@Override
	public MVMetadata getDefaultInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		//first let's try to find default metadata dynamically from component instance
		MetadataProvider metadataProvider = getMetadataProvider(engineComponent);
		if (metadataProvider != null) {
			MVMetadata metadata = metadataProvider.getInputMetadata(portIndex, metadataPropagationResolver);
			if (metadata != null) {
				return metadata;
			}
		}
		//no dynamic metadata found, let's use statical metadata from component descriptor 
		String metadataId = engineComponent.getDescriptor().getDefaultInputMetadataId(portIndex);
		return getStaticMetadata(metadataId);
	}
	
	/**
	 * @return metadata from static metadata repository
	 */
	private MVMetadata getStaticMetadata(String metadataId) {
		DataRecordMetadata metadata = MetadataRepository.getMetadata(metadataId, engineComponent);
		if (metadata != null) {
			return parentMVGraph.createMVMetadata(metadata);
		} else {
			return null;
		}
	}
	
	@Override
	public MVGraph getParentMVGraph() {
		return parentMVGraph;
	}

	@Override
	public int hashCode() {
		return engineComponent.hashCodeIdentity();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MVEngineComponent)) { 
			return false;
		}
		return engineComponent == ((MVEngineComponent) obj).engineComponent;
	}
	
	@Override
	public String toString() {
		return engineComponent.toString();
	}
}

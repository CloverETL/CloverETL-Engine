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

import java.util.HashMap;
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
import org.jetel.graph.modelview.MVSubgraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.MetadataRepository;
import org.jetel.util.SubgraphUtils;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.string.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * General model wrapper for engine component ({@link Node}).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 8. 2013
 */
public class MVEngineComponent extends MVEngineGraphElement implements MVComponent {

	private static final long serialVersionUID = -7445666999175898101L;

	private Map<Integer, MVEdge> inputEdges;

	private Map<Integer, MVEdge> outputEdges;
	
	private transient boolean metadataProviderFound = false;
	private transient MetadataProvider metadataProvider;
	
	@SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
	private transient Map<Integer, MVMetadata> inputMetadataCache = new HashMap<>();
	@SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
	private transient Map<Integer, MVMetadata> outputMetadataCache = new HashMap<>();
	
	MVEngineComponent(Node engineComponent, MVGraph parentMVGraph) {
		super(engineComponent, parentMVGraph);
		
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
		return (Node) super.getModel();
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
		return getModel().getDescriptor().isPassThrough();
	}

	/**
	 * @return metadata provider based on engine component
	 */
	private MetadataProvider getMetadataProvider() {
		if (!metadataProviderFound) {
			metadataProviderFound = true;
			Node engineComponent = getModel();
			if (engineComponent instanceof MetadataProvider) {
				metadataProvider = (MetadataProvider) engineComponent;
			} else if (engineComponent != null && !StringUtils.isEmpty(engineComponent.getDescriptor().getMetadataProvider())) {
				metadataProvider = ClassLoaderUtils.loadClassInstance(MetadataProvider.class, engineComponent.getDescriptor().getMetadataProvider(), engineComponent);
				if (metadataProvider instanceof ComponentMetadataProvider) {
					((ComponentMetadataProvider) metadataProvider).setComponent(engineComponent);
				}
			}
		}
		return metadataProvider;
	}
	
	@Override
	public MVMetadata getDefaultOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		//first let's try to find default metadata dynamically from component instance
		MetadataProvider metadataProvider = getMetadataProvider();
		if (metadataProvider != null) {
			MVMetadata metadata = metadataProvider.getOutputMetadata(portIndex, metadataPropagationResolver);
			if (metadata != null) {
				//resulted metadata are compared with cache - the cached value is returned if both metadata seems to identical
				MVMetadata oldMetadata = outputMetadataCache.get(portIndex);
				if (oldMetadata != null && equalsMetadata(metadata, oldMetadata)) {
					return oldMetadata;
				} else {
					outputMetadataCache.put(portIndex, metadata);
					return metadata;
				}
			}
		}

		//no dynamic metadata found, let's use static metadata from component descriptor 
		String metadataId = getModel().getDescriptor().getDefaultOutputMetadataId(portIndex);
		return metadataId != null ? getStaticMetadata(metadataId) : null;
	}

	@Override
	public MVMetadata getDefaultInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		//first let's try to find default metadata dynamically from component instance
		MetadataProvider metadataProvider = getMetadataProvider();
		if (metadataProvider != null) {
			MVMetadata metadata = metadataProvider.getInputMetadata(portIndex, metadataPropagationResolver);
			if (metadata != null) {
				//resulted metadata are compared with cache - the cached value is returned if both metadata seems to identical
				MVMetadata oldMetadata = inputMetadataCache.get(portIndex);
				if (oldMetadata != null && equalsMetadata(metadata, oldMetadata)) {
					return oldMetadata;
				} else {
					inputMetadataCache.put(portIndex, metadata);
					return metadata;
				}
			}
		}
		//no dynamic metadata found, let's use statical metadata from component descriptor 
		String metadataId = getModel().getDescriptor().getDefaultInputMetadataId(portIndex);
		return metadataId != null ? getStaticMetadata(metadataId) : null;
	}
	
	/**
	 * @return metadata from static metadata repository
	 */
	private MVMetadata getStaticMetadata(String metadataId) {
		DataRecordMetadata metadata = MetadataRepository.getMetadata(metadataId, getModel());
		if (metadata != null) {
			return getParent().createMVMetadata(metadata);
		} else {
			return null;
		}
	}
	
	@Override
	public MVGraph getParent() {
		return (MVGraph) super.getParent();
	}

	@Override
	public boolean isSubgraphComponent() {
		return SubgraphUtils.isSubJobComponent(getModel().getType());
	}
	
	@Override
	public boolean isSubgraphInputComponent() {
		return SubgraphUtils.isSubJobInputComponent(getModel().getType());
	}
	
	@Override
	public boolean isSubgraphOutputComponent() {
		return SubgraphUtils.isSubJobOutputComponent(getModel().getType());
	}

	/**
	 * Can return null if the Subgraph component does not have valid subgraph reference. 
	 */
	@Override
	public MVSubgraph getSubgraph() {
		if (isSubgraphComponent()) {
			return getParentMVGraph().getMVSubgraphs().get(getId());
		} else {
			throw new IllegalStateException();
		}
	}

	/**
	 * @param metadata1
	 * @param metadata2
	 * @return true if both metadata are equal - just major attributes are compared (id, names, fields count, types)
	 */
	private static boolean equalsMetadata(MVMetadata metadata1, MVMetadata metadata2) {
		if (!metadata1.getId().equals(metadata2.getId())) {
			return false;
		}
		if (metadata1.getPriority() != metadata2.getPriority()) {
			return false;
		}
		
		DataRecordMetadata dataRecordMetadata1 = metadata1.getModel();
		DataRecordMetadata dataRecordMetadata2 = metadata2.getModel();

		if (dataRecordMetadata1.getNumFields() != dataRecordMetadata2.getNumFields()) {
			return false;
		}
		
		if (!dataRecordMetadata1.getName().equals(dataRecordMetadata2.getName())) {
			return false;
		}

		for (int i = 0; i < dataRecordMetadata1.getNumFields(); i++) {
			DataFieldMetadata dataFieldMetadata1 = dataRecordMetadata1.getField(i);
			DataFieldMetadata dataFieldMetadata2 = dataRecordMetadata2.getField(i);
			if (!dataFieldMetadata1.getName().equals(dataFieldMetadata2.getName())) {
				return false;
			}
			if (!dataFieldMetadata1.getDataType().equals(dataFieldMetadata2.getDataType())) {
				return false;
			}
		}

		return true;
	}

}

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
package org.jetel.util;

import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.jetel.component.RecordTransform;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordWithInvalidState;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.TransformException;
import org.jetel.graph.Edge;
import org.jetel.graph.EdgeFactory;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.SandboxUrlUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.StringUtils;

/**
 * This is utility class for graph manipulation.  
 * 
 * The code should be moved to proper place in the future.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 21.12.2012
 * @see TransformationAnalyser
 * @see ClusteredGraphProvider
 */
public class GraphUtils {

	public static final String JOB_PARAMETERS_METADATA_NAME = "JobParameters";

	public static final String DICTIONARY_METADATA_NAME = "Dictionary";

    public static final String RUN_STATUS_RECORD_NAME = "RunStatus";

    private static final int RS_RUN_ID_INDEX = 0;
    private static final int RS_ORIGINAL_JOB_URL_INDEX = 1;
    private static final int RS_START_TIME_INDEX = 2;
    private static final int RS_END_TIME_INDEX = 3;
    private static final int RS_DURATION_INDEX = 4;
    private static final int RS_EXECUTION_GROUP_INDEX = 5;
    private static final int RS_EXECUTION_LABEL_INDEX = 6;
    private static final int RS_STATUS_INDEX = 7;
    private static final int RS_ERR_EXCEPTION_INDEX = 8;
    private static final int RS_ERR_MESSAGE_INDEX = 9;
    private static final int RS_ERR_COMPONENT_INDEX = 10;
    private static final int RS_ERR_COMPONENT_TYPE_INDEX = 11;
    
    private static final String RS_RUN_ID_NAME = "runId";
    private static final String RS_ORIGINAL_JOB_URL_NAME = "originalJobURL";
    private static final String RS_START_TIME_NAME = "startTime";
    private static final String RS_END_TIME_NAME = "endTime";
    private static final String RS_DURATION_NAME = "duration";
    private static final String RS_EXECUTION_GROUP_NAME = "executionGroup";
    public static final String RS_EXECUTION_LABEL_NAME = "executionLabel";
    public static final String RS_STATUS_NAME = "status";
    private static final String RS_ERR_EXCEPTION_NAME = "errException";
    private static final String RS_ERR_MESSAGE_NAME = "errMessage";
    private static final String RS_ERR_COMPONENT_NAME = "errComponent";
    private static final String RS_ERR_COMPONENT_TYPE_NAME = "errComponentType";
    
    public static final String PUBLIC_GRAPH_PARAMETER_ATTRIBUTE = "public";
    public static final String REQUIRED_GRAPH_PARAMETER_ATTRIBUTE = "required";

    
	/**
	 * Inserts the given component into the given edge.
	 */
	public static void insertComponent(Node insertedComponent, Edge edge) {
		TransformationGraph graph = edge.getGraph();
		
		//insert component into correct phase
		Phase phase = edge.getWriter().getPhase();
		phase.addNode(insertedComponent);
		
		//create the left artificial edge
		Edge leftEdge = EdgeFactory.newEdge(edge.getId() + "_inserted", edge.getMetadata());
		Node writer = edge.getWriter();
		Node reader = insertedComponent;
		writer.addOutputPort(edge.getOutputPortNumber(), leftEdge);
		reader.addInputPort(0, leftEdge);
		try {
			graph.addEdge(leftEdge);
		} catch (GraphConfigurationException e) {
			throw new JetelRuntimeException("Component '" + insertedComponent + "' cannot be inserted into graph.", e);
		}
		
		//re-attach the edge
		writer = insertedComponent;
		reader = edge.getReader();
		writer.addOutputPort(0, edge);
		reader.addInputPort(edge.getInputPortNumber(), edge);
	}
	
	/**
	 * Removes the component from graph. Component has
	 * to have equal number of input and output edges.
	 * These edges are re-connected in 'pass-through' way.
	 * @param component component to be removed from graph
	 */
	public static void removeComponent(Node component) {
		if (component.getInPorts().size() == component.getOutPorts().size()) {
			TransformationGraph graph = component.getGraph();
			
			for (int i = 0; i < component.getInPorts().size(); i++) {
				Edge leftEdge = (Edge) component.getInputPort(i);
				Edge rightEdge = (Edge) component.getOutputPort(i);
				Node rightComponent = rightEdge.getReader();
				
				//remove the right edge
				try {
					graph.deleteEdge(rightEdge);
				} catch (GraphConfigurationException e) {
					throw new JetelRuntimeException("Component '" + component + "' cannot be removed from graph.", e);
				}
				
				//re-attach the left edge
				rightComponent.addInputPort(rightEdge.getInputPortNumber(), leftEdge);
			}

			//remove component from phase
			component.getPhase().deleteNode(component);
		} else {
			throw new JetelRuntimeException("Component '" + component + "' cannot be removed from graph. Number of input edges is not equal to number of output edges.");
		}
	}
	

	/**
	 * The graph duplicate is not valid graph, only basic structure of the graph
	 * is duplicated. The duplicate is used for graph cycle detection in clustered graphs
	 * and few other places.
	 * @param templateGraph graph which is duplicated
	 * @return structural copy of the given graph
	 */
	public static TransformationGraph duplicateGraph(TransformationGraph templateGraph) {
		TransformationGraph graph = new TransformationGraph(templateGraph.getId());
		graph.setStaticJobType(templateGraph.getStaticJobType());
		graph.setInitialRuntimeContext(templateGraph.getRuntimeContext().createCopy());
		
		try {
			for (Phase templatePhase : templateGraph.getPhases()) {
				duplicatePhase(graph, templatePhase);
			}

			for (Edge templateEdge : templateGraph.getEdges().values()) {
				duplicateEdge(graph, templateEdge);
			}
		} catch (GraphConfigurationException e) {
			throw new JetelRuntimeException("Graph cannot be duplicated.", e);
		}
		
		return graph;
	}

	/**
	 * @param graph
	 * @param templatePhase
	 * @throws GraphConfigurationException 
	 */
	private static void duplicatePhase(TransformationGraph graph, Phase templatePhase) throws GraphConfigurationException {
		Phase phase = new Phase(templatePhase.getPhaseNum());
		graph.addPhase(phase);
		for (Node templateComponent : templatePhase.getNodes().values()) {
			duplicateComponent(phase, templateComponent);
		}
	}

	/**
	 * @param phase
	 * @param templateEdge
	 * @throws GraphConfigurationException 
	 */
	private static void duplicateEdge(TransformationGraph graph, Edge templateEdge) throws GraphConfigurationException {
		Edge edge = EdgeFactory.newEdge(templateEdge.getId(), templateEdge);
		Node writer = graph.getNodes().get(templateEdge.getWriter().getId());
		Node reader = graph.getNodes().get(templateEdge.getReader().getId());
		writer.addOutputPort(templateEdge.getOutputPortNumber(), edge);
		reader.addInputPort(templateEdge.getInputPortNumber(), edge);
		graph.addEdge(edge);
	}

	/**
	 * @param templateComponent
	 * @return
	 */
	private static void duplicateComponent(Phase phase, Node templateComponent) {
		ComponentMockup component = new ComponentMockup(templateComponent.getId(), templateComponent.getType());
		component.setName(templateComponent.getName());
		component.setEnabled(templateComponent.getEnabled());
		component.setAllocation(templateComponent.getAllocation());
		component.setUsedUrls(templateComponent.getUsedUrls());
		phase.addNode(component);
	}
	
	private static class ComponentMockup extends Node {
		private String type;
		private String[] usedUrls;
		public ComponentMockup(String id, String type) {
			super(id);
			this.type = type;
		}
		
		@Override
		public String getType() {
			return type;
		}

		@Override
		protected Result execute() throws Exception {
			return null;
		}
		
		public void setUsedUrls(String[] usedUrls) {
			this.usedUrls = usedUrls;
		}
		
		@Override
		public String[] getUsedUrls() {
			return usedUrls;
		}
	}

	/**
	 * Finds unique identifier for a component in the given graph.
	 * The identifier is derived from the suggestedId parameter.
	 * @param graph
	 * @param clusterRegatherType
	 * @return
	 */
	public static String getUniqueComponentId(TransformationGraph graph, String suggestedId) {
		if (isUniqueComponentId(graph, suggestedId)) {
			return suggestedId;
		}
		
		int i = 1;
		String newSuggestedId = null;
		do {
			newSuggestedId = suggestedId + (i++);
		} while(!isUniqueComponentId(graph, newSuggestedId));
		return newSuggestedId;
	}

	private static boolean isUniqueComponentId(TransformationGraph graph, String suggestedId) {
		for (Node component : graph.getNodes().values()) {
			if (component.getId().equals(suggestedId)) {
				return false;
			}
		}
		return true;
	}

	
	private static EdgeTypeEnum[][] edgeCombinations;

	private static EdgeTypeEnum[][] getEdgeCombinations() {
		if (edgeCombinations == null) {
			 edgeCombinations = new EdgeTypeEnum[EdgeTypeEnum.values().length][EdgeTypeEnum.values().length];

			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.DIRECT;
			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.BUFFERED;
			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.DIRECT_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.DIRECT.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;

			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.BUFFERED;
			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.BUFFERED;
			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.BUFFERED.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;

			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.PHASE_CONNECTION.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;

			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.DIRECT_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.DIRECT_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;

			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.DIRECT.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.BUFFERED.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.PHASE_CONNECTION.ordinal()] = EdgeTypeEnum.PHASE_CONNECTION;
			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.DIRECT_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
			 edgeCombinations[EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()][EdgeTypeEnum.BUFFERED_FAST_PROPAGATE.ordinal()] = EdgeTypeEnum.BUFFERED_FAST_PROPAGATE;
		}
		return edgeCombinations;
	}

	/**
	 * This method derives from two edge types an edge type which should satisfy needs from both.
	 */
	public static EdgeTypeEnum combineEdges(EdgeTypeEnum edgeType1, EdgeTypeEnum edgeType2) {
		EdgeTypeEnum result = getEdgeCombinations()[edgeType1.ordinal()][edgeType2.ordinal()];
		if (result != null) {
			return result;
		} else {
			throw new IllegalArgumentException("unexpected edge types for combination " + edgeType1 + " " + edgeType2);
		}
	}

	/**
	 * @param source reader component of expected edge
	 * @param target writer component of expected edge
	 * @return true if an edge from source to target component exists
	 */
	public static boolean hasEdge(Node source, Node target) {
		for (OutputPort outputPort : source.getOutPorts()) {
			if (outputPort.getEdge().getReader() == target) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Creates metadata based on graph parameters of the graph.
	 */
	public static DataRecordMetadata createMetadataFromGraphParameters(TransformationGraph graph) {
		if (graph != null) {
			DataRecordMetadata metadata = new DataRecordMetadata(JOB_PARAMETERS_METADATA_NAME);
			
			List<GraphParameter> parameters = graph.getGraphParameters().getAllGraphParameters();
			
			for (GraphParameter graphParameter : parameters) {
				DataFieldMetadata field = new DataFieldMetadata("_", DataFieldType.STRING, null);
				field.setLabel(graphParameter.getName());
				//set description to be shown in tooltip
				String description = "Default value: '" + graphParameter.getValueRaw() + "'";
				if (!StringUtils.isEmpty(graphParameter.getDescription())) {
					description += "\nDescription: " + graphParameter.getDescription(); 
				}
				field.setDescription(description);
				if (graphParameter.isPublic()) {
					// in the GUI code it is too late because of normalization
					field.setProperty(PUBLIC_GRAPH_PARAMETER_ATTRIBUTE, Boolean.TRUE.toString());
				}
				if (graphParameter.isRequired()) {
					// in the GUI code it is too late because of normalization
					field.setProperty(REQUIRED_GRAPH_PARAMETER_ATTRIBUTE, Boolean.TRUE.toString());
				}
				metadata.addField(field);
			}
			
			metadata.normalize();
			
			for (int i = 0; i < parameters.size(); i++) {
				// override the labels after normalize()
				metadata.getField(i).setLabel(parameters.get(i).getLabelOrName());
			}
	
			return metadata.getFields().length > 0 ? metadata : null;
		} else {
			return null;
		}
	}

	/**
	 * Populates the given record by data from graph parameters of the given graph.
	 * @param graph graph parameters of this graph are source for record population
	 * @param record populated record
	 */
	public static void populateRecordFromGraphParameters(TransformationGraph graph, DataRecord record) {
		if (graph != null && record != null) {
			TypedProperties graphParameters = graph.getGraphParameters().asProperties();
			populateRecordFromProperties(graphParameters, record);
		}
	}

	/**
	 * Populates the given record by data from the given properties.
	 * @param properties source of data
	 * @param record populated record
	 */
	public static void populateRecordFromProperties(TypedProperties properties, DataRecord record) {
		if (properties != null && record != null) {
			for (DataField field : record) {
				String fieldName = field.getMetadata().getName();
				if (field.getMetadata().getDataType() == DataFieldType.STRING
						&& properties.containsKey(fieldName)) {
					String graphParameter = properties.getProperty(fieldName);
					field.setValue(graphParameter);
				}
			}
		}
	}

	/**
	 * Creates data record metadata based on dictionary of given transformation graph.
	 * @param graph 
	 * @param onlyInput <code>true</code> if only input dictionary entries should be considered;
	 * <code>false</code> if only output dictionary entries should be considered; null if all entries
	 * should be considered
	 * @return
	 */
	public static DataRecordMetadata createMetadataFromDictionary(TransformationGraph graph, Boolean onlyInput) {
		if (graph != null) {
			DataRecordMetadata metadata = new DataRecordMetadata(DICTIONARY_METADATA_NAME);
			
			Dictionary dictionary = graph.getDictionary();
			List<DataFieldMetadata> fields = new ArrayList<DataFieldMetadata>();
			for (String entryName : dictionary.getKeys()) {
				if (onlyInput == null
						|| ((onlyInput && dictionary.isInput(entryName))
								|| (!onlyInput && dictionary.isOutput(entryName)))) {
					DataFieldType fieldType = dictionary.getType(entryName).getFieldType(dictionary.getContentType(entryName));
					DataFieldContainerType fieldContainerType = dictionary.getType(entryName).getFieldContainerType();
					if (fieldType != null && fieldContainerType != null) {
						DataFieldMetadata field = new DataFieldMetadata("_", fieldType, null, fieldContainerType);
						field.setLabel(entryName);
						//set description, which will be shown in tooltip
						Object defaultValue = dictionary.getValue(entryName);
						if (defaultValue != null) {
							field.setDescription("Default: " + defaultValue.toString());
						}
						fields.add(field);
					}
				}
			}
			
			//sort fields
			Collections.sort(fields, new Comparator<DataFieldMetadata>() {
				@Override
				public int compare(DataFieldMetadata field1, DataFieldMetadata field2) {
					return field1.getLabel().compareTo(field2.getLabel());
				}
			});
			
			//add sorted fields into metadata
			for (DataFieldMetadata field : fields) {
				metadata.addField(field);
			}

			metadata.normalize();
	
			return metadata.getFields().length > 0 ? metadata : null;
		} else {
			return null;
		}
	}

	/**
	 * Populates given record by default values from a graph dictionary. 
	 * @param dictionary dictionary is source for record population
	 * @param record record to populate
	 */
	public static void populateRecordFromDictionary(Dictionary dictionary, DataRecord record) {
		if (record != null) {
			//initialize dictionary if necessary
			if (!dictionary.isInitialized()) {
				try {
					dictionary.init();
				} catch (ComponentNotReadyException e) {
					throw new JetelRuntimeException("Dictionary initialization failed. Default dictionary values are not available.");
				}
			}
			for (DataField field : record) {
				String entryName = field.getMetadata().getLabelOrName();
				if (dictionary.hasEntry(entryName)) {
					if (field.getMetadata().getDataType().equals(dictionary.getType(entryName).getFieldType(dictionary.getContentType(entryName)))
							&& field.getMetadata().getContainerType().equals(dictionary.getType(entryName).getFieldContainerType())) {
						field.setValue(dictionary.getValue(entryName));
					}
				}
			}
		}
	}

	/**
	 * @return transformation graph instance defined in the given location (only graph parameters and dictionary are loaded)
	 */
	public static TransformationGraph createGraphAsInterface(URL contextUrl, String fileUrl, GraphRuntimeContext runtimeContext) {
		return createGraph(contextUrl, fileUrl, runtimeContext, true, false, false);
	}

	/**
	 * @return transformation graph instance defined in the given location (automatic metadata propagation is turned off)
	 */
	public static TransformationGraph createGraphNoMetadataPropagation(URL contextUrl, String fileUrl, GraphRuntimeContext runtimeContext) {
		return createGraph(contextUrl, fileUrl, runtimeContext, false, false, false);
	}

	/**
	 * @return transformation graph instance defined in the given location
	 */
	public static TransformationGraph createGraphWithMetadataPropagation(URL contextUrl, String fileUrl, GraphRuntimeContext runtimeContext) {
		return createGraph(contextUrl, fileUrl, runtimeContext, false, true, true);
	}

	private static TransformationGraph createGraph(URL contextUrl, String fileUrl, GraphRuntimeContext runtimeContext, boolean onlyParamsAndDict, boolean metadataPropagation, boolean strictParsing) {
		if (FileUtils.isMultiURL(fileUrl)) {
			throw new JetelRuntimeException("Only simple job URL is allowed (" + fileUrl + ").");
		}
		InputStream in = null;
		try {
			//if the fileUrl is absolute path to a sandbox, contextURL of loaded graph has to be updated
			if (SandboxUrlUtils.isSandboxUrl(fileUrl)) {
				//for example for fileURL="sandbox:/project/graph/myGraph.grf" is contextURL="sandbox:/project/"
				runtimeContext.setContextURL(SandboxUrlUtils.getSandboxUrl(SandboxUrlUtils.getSandboxName(fileUrl)));
			}
			runtimeContext.setJobUrl(FileUtils.getFileURL(contextUrl, fileUrl).toString());
			
	        TransformationGraphXMLReaderWriter graphReader = new TransformationGraphXMLReaderWriter(runtimeContext);
	        graphReader.setStrictParsing(strictParsing);
	        graphReader.setOnlyParamsAndDict(onlyParamsAndDict);
	        graphReader.setMetadataPropagation(metadataPropagation);
	        in = FileUtils.getInputStream(contextUrl, fileUrl);
	        return graphReader.read(in);
		} catch (Exception e) {
			throw new JetelRuntimeException("Job '" + fileUrl + "' cannot be loaded. ", e);
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	/**
	 * Performs the given transformation. OnError method is invoked if something goes wrong.
	 */
	public static int performTransformation(RecordTransform transformation, DataRecord[] inRecords, DataRecord[] outRecords, String errMessage) {
		try {
			return transformation.transform(inRecords, outRecords);
		} catch (Exception exception) {
			try {
				return transformation.transformOnError(exception, inRecords, outRecords);
			} catch (TransformException e) {
				throw new JetelRuntimeException(errMessage, e);
			}
		}
	}
	
	/**
	 * Populates the given record based on dictionary content passed by {@link RunStatus}
	 */
	public static void populateDictionaryRecordFromRunStatus(DataRecord outputDictionaryRecord, RunStatus runStatus) {
		if (outputDictionaryRecord != null) {
			outputDictionaryRecord.reset();
			if (outputDictionaryRecord instanceof DataRecordWithInvalidState) {
				//mark all fields as invalid, see CLO-1872
				//only populated fields will be have valid value
				((DataRecordWithInvalidState) outputDictionaryRecord).setValid(false);
			}
			DictionaryValuesContainer dictionaryContent = runStatus.dictionaryOut;
			if (dictionaryContent != null) {
				for (Entry<String, Serializable> entry : dictionaryContent.getContent().entrySet()) {
					if (outputDictionaryRecord.hasField(entry.getKey())) {
						outputDictionaryRecord.getField(entry.getKey()).setValue(entry.getValue());
					}
				}
			}
		}
	}
	
	/**
	 * Populates the given record based on tracking information passed by {@link RunStatus}
	 */
	public static void populateTrackingRecordFromRunStatus(DataRecord trackingRecord, RunStatus runStatus) {
		if (trackingRecord != null) {
			trackingRecord.reset();
			if (trackingRecord instanceof DataRecordWithInvalidState) {
				//mark all fields as invalid, see CLO-1872
				//only populated fields will be have valid value
				((DataRecordWithInvalidState) trackingRecord).setValid(false);
			}
			if (runStatus.tracking != null) {
				try {
					TrackingMetadataToolkit.populateTrackingRecord(trackingRecord, runStatus.tracking);
				} catch (Exception e) {
					throw new JetelRuntimeException("Tracking record population failed.", e);
				}
			}
		}
	}

	/**
	 * Populates the given record based {@link RunStatus} object.
	 */
	public static void populateRecordFromRunStatus(DataRecord runStatusRecord, RunStatus runStatus) {
		runStatusRecord.getField(RS_RUN_ID_INDEX).setValue(runStatus.runId);
		runStatusRecord.getField(RS_ORIGINAL_JOB_URL_INDEX).setValue(runStatus.jobUrl);
		runStatusRecord.getField(RS_START_TIME_INDEX).setValue(runStatus.startTime);
		runStatusRecord.getField(RS_END_TIME_INDEX).setValue(runStatus.endTime);
		runStatusRecord.getField(RS_DURATION_INDEX).setValue(runStatus.duration);
		runStatusRecord.getField(RS_EXECUTION_GROUP_INDEX).setValue(runStatus.executionGroup);
		runStatusRecord.getField(RS_EXECUTION_LABEL_INDEX).setValue(runStatus.executionLabel);
		runStatusRecord.getField(RS_STATUS_INDEX).setValue(runStatus.status != null ? runStatus.status.message() : null);
		runStatusRecord.getField(RS_ERR_EXCEPTION_INDEX).setValue(runStatus.errException);
		runStatusRecord.getField(RS_ERR_MESSAGE_INDEX).setValue(runStatus.errMessage);
		runStatusRecord.getField(RS_ERR_COMPONENT_INDEX).setValue(runStatus.errComponent);
		runStatusRecord.getField(RS_ERR_COMPONENT_TYPE_INDEX).setValue(runStatus.errComponentType);
	}
	
	/**
	 * @return metadata for {@link RunStatus}
	 */
	public static DataRecordMetadata createRunStatusMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(RUN_STATUS_RECORD_NAME);
		
		metadata.addField(RS_RUN_ID_INDEX, new DataFieldMetadata(RS_RUN_ID_NAME, DataFieldType.LONG, null));
		metadata.addField(RS_ORIGINAL_JOB_URL_INDEX, new DataFieldMetadata(RS_ORIGINAL_JOB_URL_NAME, DataFieldType.STRING, null));
		metadata.addField(RS_START_TIME_INDEX, new DataFieldMetadata(RS_START_TIME_NAME, DataFieldType.DATE, null));
		metadata.addField(RS_END_TIME_INDEX, new DataFieldMetadata(RS_END_TIME_NAME, DataFieldType.DATE, null));
		metadata.addField(RS_DURATION_INDEX, new DataFieldMetadata(RS_DURATION_NAME, DataFieldType.LONG, null));
		metadata.addField(RS_EXECUTION_GROUP_INDEX, new DataFieldMetadata(RS_EXECUTION_GROUP_NAME, DataFieldType.STRING, null));
		metadata.addField(RS_EXECUTION_LABEL_INDEX, new DataFieldMetadata(RS_EXECUTION_LABEL_NAME, DataFieldType.STRING, null));
		metadata.addField(RS_STATUS_INDEX, new DataFieldMetadata(RS_STATUS_NAME, DataFieldType.STRING, null));
		metadata.addField(RS_ERR_EXCEPTION_INDEX, new DataFieldMetadata(RS_ERR_EXCEPTION_NAME, DataFieldType.STRING, null));
		metadata.addField(RS_ERR_MESSAGE_INDEX, new DataFieldMetadata(RS_ERR_MESSAGE_NAME, DataFieldType.STRING, null));
		metadata.addField(RS_ERR_COMPONENT_INDEX, new DataFieldMetadata(RS_ERR_COMPONENT_NAME, DataFieldType.STRING, null));
		metadata.addField(RS_ERR_COMPONENT_TYPE_INDEX, new DataFieldMetadata(RS_ERR_COMPONENT_TYPE_NAME, DataFieldType.STRING, null));

		return metadata;
	}

	/**
	 * Data values from given data records are copied to the given graph dictionary.
	 * @param dictionary populated graph dictionary
	 * @param dictionaryRecord source data record 
	 */
	public static void populateDictionaryFromRecord(Dictionary dictionary, CTLMapping mapping, String dictionaryRecordName) {
		DataRecord dictionaryRecord = mapping.getOutputRecord(dictionaryRecordName);
		if (dictionaryRecord != null) {
			for (DataField field : dictionaryRecord) {
				if (mapping.isOutputOverridden(dictionaryRecord, field)) {
					Object val = field.getValueDuplicate();
					try {
						dictionary.setValue(field.getMetadata().getLabelOrName(), (Serializable) val);
					} catch (ComponentNotReadyException e) {
						throw new JetelRuntimeException("Dictionary entry '" + field.getMetadata().getLabelOrName() + "' cannot be populated with '" + val + "'.", e);
					}
				}
			}
		}
	}

	/**
	 * Converst all edges between the given components to fast-propagating type.
	 * Direct edge is changed to direct fast propagated edge,
	 * buffered edge is changed to buffered fast propagated edge,
	 * an exception is thrown for a phase edge.
	 * @param components
	 */
	public static void makeEdgesFastPropagate(Collection<Node> components) {
		for (Node component : components) {
			for (OutputPort outputPort : component.getOutPorts()) {
				Edge edge = outputPort.getEdge();
				if (components.contains(edge.getReader())) {
					setEdgeAsFastPropagate(edge);
				}
			}
		}
	}
	
	/**
	 * Converts the given edge to phase propagated type.
	 * Direct edge is changed to direct fast propagated edge,
	 * buffered edge is changed to buffered fast propagated edge,
	 * an exception is thrown for a phase edge.
	 * @param edge
	 */
	public static void setEdgeAsFastPropagate(Edge edge) {
		if (edge.getEdgeType() == EdgeTypeEnum.DIRECT || edge.getEdgeType() == EdgeTypeEnum.DIRECT_FAST_PROPAGATE) {
			edge.setEdgeType(EdgeTypeEnum.DIRECT_FAST_PROPAGATE);
		} else if (edge.getEdgeType() == EdgeTypeEnum.BUFFERED || edge.getEdgeType() == EdgeTypeEnum.BUFFERED_FAST_PROPAGATE) {
			edge.setEdgeType(EdgeTypeEnum.BUFFERED_FAST_PROPAGATE);
		} else {
			throw new JetelRuntimeException("Unexpected edge type (" + edge.getId() + ":" + edge.getEdgeType() + ").");
		}
	}

}

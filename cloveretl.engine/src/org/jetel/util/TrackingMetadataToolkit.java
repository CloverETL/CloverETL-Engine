/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.DataRecord;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.jmx.GraphTracking;
import org.jetel.graph.runtime.jmx.GraphTrackingDetail;
import org.jetel.graph.runtime.jmx.InputPortTracking;
import org.jetel.graph.runtime.jmx.NodeTracking;
import org.jetel.graph.runtime.jmx.OutputPortTracking;
import org.jetel.graph.runtime.jmx.PhaseTracking;
import org.jetel.graph.runtime.jmx.PortTracking;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This class is simple toolkit for generation and manipulation with tracking metadata and records.
 * Tracking metadata are artificial data structure containg all tracking information of a graph.
 * 
 * For conversion a TrasnformationGraph to tracking metadata call {@link #createMetadadata(TransformationGraph)}.
 * 
 * GraphTacking object can be used also for generation of tracking metadata via invocation
 * of {@link #createMetadadata(GraphTracking)} method.
 * 
 * Tracking data record can be populated based on GraphTracking object using 
 * {@link #populateTrackingRecord(DataRecord, GraphTracking)} method.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2 Mar 2012
 */
public class TrackingMetadataToolkit {

	public static final String METADATA_NAME = "Tracking";
	
	public static final String METADATA_ANNOTATION_NAME = "com.opensys.cloveretl.component.jobflow.TrackingMetadataToolkit";
	public static final String TRACKING_METADATA_ANNOTATION_VALUE = "tracking";

	public static final String IN = "in";
	public static final String OUT = "out";

	/**
	 * Fields generated in tracking metadata can be categorised into several groups:<br>
	 * GRAPH, PHASE, COMPONENT, INPUT_PORT and OUTPUT_PORT<br>
	 * This class is mainly used in backward analyse of field names.
	 * 
	 */
	public static enum FieldType {
		GRAPH("graph", "graph_(.*)") {
			@Override
			public FieldNameStructure tryToAnalyseFieldName(String fieldName) {
				Matcher matcher = FieldType.GRAPH.getFieldNameMatcher(fieldName);
				if (matcher.matches()) {
					Attribute attribute = Attribute.fromString(matcher.group(1));
					return FieldNameStructure.createGraphFieldName(attribute);
				} else {
					return null;
				}
			}
		},
		PHASE("phase", "phase_(.*?)_(.*)") {
			@Override
			public FieldNameStructure tryToAnalyseFieldName(String fieldName) {
				Matcher matcher = FieldType.PHASE.getFieldNameMatcher(fieldName);
				if (matcher.matches()) {
					int index;
					try {
						index = Integer.parseInt(matcher.group(1));
					} catch (NumberFormatException e) {
						throw new JetelRuntimeException("unsupported field name " + fieldName);
					}
					Attribute attribute = Attribute.fromString(matcher.group(2));
					return FieldNameStructure.createPhaseFieldName(index, attribute);
				} else {
					return null;
				}
			}
		},
		COMPONENT("component", "component_(.*)_(.*)") {
			@Override
			public FieldNameStructure tryToAnalyseFieldName(String fieldName) {
				Matcher matcher = FieldType.COMPONENT.getFieldNameMatcher(fieldName);
				if (matcher.matches()) {
					String componentId = matcher.group(1);
					Attribute attribute = Attribute.fromString(matcher.group(2));
					return FieldNameStructure.createComponentFieldName(componentId, attribute);
				} else {
					return null;
				}
			}
		},
		INPUT_PORT("inputPort", "inputPort_(.*?)_(.*)_(.*)") {
			@Override
			public FieldNameStructure tryToAnalyseFieldName(String fieldName) {
				Matcher matcher = FieldType.INPUT_PORT.getFieldNameMatcher(fieldName);
				if (matcher.matches()) {
					int index;
					try {
						index = Integer.parseInt(matcher.group(1));
					} catch (NumberFormatException e) {
						throw new JetelRuntimeException("unsupported field name " + fieldName);
					}
					String componentId = matcher.group(2);
					Attribute attribute = Attribute.fromString(matcher.group(3));
					return FieldNameStructure.createInputPortFieldName(index, componentId, attribute);
				} else {
					return null;
				}
			}
		},
		OUTPUT_PORT("outputPort", "outputPort_(.*?)_(.*)_(.*)") {
			@Override
			public FieldNameStructure tryToAnalyseFieldName(String fieldName) {
				Matcher matcher = FieldType.OUTPUT_PORT.getFieldNameMatcher(fieldName);
				if (matcher.matches()) {
					int index;
					try {
						index = Integer.parseInt(matcher.group(1));
					} catch (NumberFormatException e) {
						throw new JetelRuntimeException("unsupported field name " + fieldName);
					}
					String componentId = matcher.group(2);
					Attribute attribute = Attribute.fromString(matcher.group(3));
					return FieldNameStructure.createOutputPortFieldName(index, componentId, attribute);
				} else {
					return null;
				}
			}
		};
		
		private String name;
		private Pattern pattern;
		private FieldType(String name, String pattern) {
			this.name = name;
			this.pattern = Pattern.compile(pattern);
		}
		private Matcher getFieldNameMatcher(String fieldName) {
			return pattern.matcher(fieldName);
		}
		/**
		 * Tries to analyse given field name and returns respective field name structure or null
		 * if given field name does represents this tracking field type. 
		 */
		abstract public FieldNameStructure tryToAnalyseFieldName(String fieldName);
		@Override
		public String toString() {
			return name;
		}
	}

	/**
	 * Enumeration of all possible tracking attributes on all levels - GRAPH, PHASE, COMPONENT, PORT.
	 */
	public static enum Attribute {
		START_TIME("startTime"),
		END_TIME("endTime"),
		EXECUTION_TIME("executionTime"),
		GRAPH_NAME("graphName"),
		RESULT("result"),
		RUNNING_PHASE("runningPhase"),
		USED_MEMORY("usedMemory"),
		MEMORY_UTILIZATION("memoryUtilization"),
		NAME("name"),
		USAGE_CPU("usageCPU"),
		USAGE_USER("usageUser"),
		PEAK_USAGE_CPU("peakUsageCPU"),
		PEAK_USAGE_USER("peakUsageUser"),
		TOTAL_CPU_TIME("totalCPUTime"),
		TOTAL_USER_TIME("totalUserTime"),
		BYTE_FLOW("byteFlow"),
		BYTE_PEAK("bytePeak"),
		TOTAL_BYTES("totalBytes"),
		RECORD_FLOW("recordFlow"),
		RECORD_PEAK("recordPeak"),
		TOTAL_RECORDS("totalRecords"),
		WAITING_RECORDS("waitingRecords"),
		AVERAGE_WAITING_RECORDS("averageWaitingRecords");
		private String name;
		private Attribute(String name) {
			this.name = name;
		}
		public static Attribute fromString(String attributeName) {
			for (Attribute attr : values()) {
				if (attr.name.equals(attributeName)) {
					return attr;
				}
			}
			throw new JetelRuntimeException("attribute does not exist " + attributeName);
		}
		@Override
		public String toString() {
			return name;
		}
	}
	
	/**
	 * Create tracking metadata based on transformation graph.
	 * @param graph model for requested tracking metadata
	 * @return tracking metadata based on the given transformation graph
	 */
	public static DataRecordMetadata createMetadata(TransformationGraph graph) {
		DataRecordMetadata metadata = createEmptyTrackingMetadata();
		
		GraphTracking graphTracking = new GraphTrackingDetail(graph);

		metadataForGraph(graphTracking, metadata);
		
		return metadata;
	}

	/**
	 * Create tracking metadata based on {@link GraphTracking}.
	 * @param graphTracking model for requested tracking metadata
	 * @return tracking metadata based on the given graph tracking object
	 */
	public static DataRecordMetadata createMetadata(GraphTracking graphTracking) {
		DataRecordMetadata metadata = createEmptyTrackingMetadata();

		metadataForGraph(graphTracking, metadata);
		
		return metadata;
	}

	public static DataRecordMetadata createEmptyTrackingMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(METADATA_NAME);
		//resulted metadata is annotated to be able to detect that this metadata are based on graph tracking
		metadata.getRecordProperties().setProperty(METADATA_ANNOTATION_NAME, TRACKING_METADATA_ANNOTATION_VALUE);
		return metadata;
	}
	
	private static void metadataForGraph(GraphTracking graphTracking, DataRecordMetadata metadata) {
		attachField(metadata, composeFieldName(graphTracking, Attribute.START_TIME), DataFieldType.LONG);
		attachField(metadata, composeFieldName(graphTracking, Attribute.END_TIME), DataFieldType.LONG);
		attachField(metadata, composeFieldName(graphTracking, Attribute.EXECUTION_TIME), DataFieldType.LONG);
		attachField(metadata, composeFieldName(graphTracking, Attribute.GRAPH_NAME), DataFieldType.STRING);
		attachField(metadata, composeFieldName(graphTracking, Attribute.RESULT), DataFieldType.STRING);
		attachField(metadata, composeFieldName(graphTracking, Attribute.RUNNING_PHASE), DataFieldType.INTEGER);
		attachField(metadata, composeFieldName(graphTracking, Attribute.USED_MEMORY), DataFieldType.INTEGER);

		for (PhaseTracking phase : graphTracking.getPhaseTracking()) {
			metadataForPhase(phase, metadata);
		}
	}

	private static void metadataForPhase(PhaseTracking phaseTracking, DataRecordMetadata metadata) {
		attachField(metadata, composeFieldName(phaseTracking, Attribute.START_TIME), DataFieldType.LONG);
		attachField(metadata, composeFieldName(phaseTracking, Attribute.END_TIME), DataFieldType.LONG);
		attachField(metadata, composeFieldName(phaseTracking, Attribute.EXECUTION_TIME), DataFieldType.LONG);
		attachField(metadata, composeFieldName(phaseTracking, Attribute.MEMORY_UTILIZATION), DataFieldType.LONG);
		attachField(metadata, composeFieldName(phaseTracking, Attribute.RESULT), DataFieldType.STRING);

		for (NodeTracking node : phaseTracking.getNodeTracking()) {
			metadataForNode(node, metadata);
		}
	}

	private static void metadataForNode(NodeTracking nodeTracking, DataRecordMetadata metadata) {
		attachField(metadata, composeFieldName(nodeTracking, Attribute.NAME), DataFieldType.STRING);
		attachField(metadata, composeFieldName(nodeTracking, Attribute.USAGE_CPU), DataFieldType.NUMBER);
		attachField(metadata, composeFieldName(nodeTracking, Attribute.USAGE_USER), DataFieldType.NUMBER);
		attachField(metadata, composeFieldName(nodeTracking, Attribute.PEAK_USAGE_CPU), DataFieldType.NUMBER);
		attachField(metadata, composeFieldName(nodeTracking, Attribute.PEAK_USAGE_USER), DataFieldType.NUMBER);
		attachField(metadata, composeFieldName(nodeTracking, Attribute.TOTAL_CPU_TIME), DataFieldType.LONG);
		attachField(metadata, composeFieldName(nodeTracking, Attribute.TOTAL_USER_TIME), DataFieldType.LONG);
		attachField(metadata, composeFieldName(nodeTracking, Attribute.RESULT), DataFieldType.STRING);
		attachField(metadata, composeFieldName(nodeTracking, Attribute.USED_MEMORY), DataFieldType.INTEGER);

		for (InputPortTracking port : nodeTracking.getInputPortTracking()) {
			metadataForPort(port, metadata, true);
		}
		
		for (OutputPortTracking port : nodeTracking.getOutputPortTracking()) {
			metadataForPort(port, metadata, false);
		}
	}

	private static void metadataForPort(PortTracking portTracking, DataRecordMetadata metadata, boolean isInput) {
		attachField(metadata, composeFieldName(portTracking, Attribute.BYTE_FLOW, isInput), DataFieldType.INTEGER);
		attachField(metadata, composeFieldName(portTracking, Attribute.BYTE_PEAK, isInput), DataFieldType.INTEGER);
		attachField(metadata, composeFieldName(portTracking, Attribute.TOTAL_BYTES, isInput), DataFieldType.LONG);
		attachField(metadata, composeFieldName(portTracking, Attribute.RECORD_FLOW, isInput), DataFieldType.INTEGER);
		attachField(metadata, composeFieldName(portTracking, Attribute.RECORD_PEAK, isInput), DataFieldType.INTEGER);
		attachField(metadata, composeFieldName(portTracking, Attribute.TOTAL_RECORDS, isInput), DataFieldType.LONG);
		attachField(metadata, composeFieldName(portTracking, Attribute.WAITING_RECORDS, isInput), DataFieldType.INTEGER);
		attachField(metadata, composeFieldName(portTracking, Attribute.AVERAGE_WAITING_RECORDS, isInput), DataFieldType.INTEGER);
		attachField(metadata, composeFieldName(portTracking, Attribute.USED_MEMORY, isInput), DataFieldType.INTEGER);
	}
	
	private static void attachField(DataRecordMetadata metadata, String name, DataFieldType type) {
		metadata.addField(new DataFieldMetadata(name, type, null));
	}

	/**
	 * Populates given data record with data given by {@link GraphTracking} object.
	 * @param record populated data record
	 * @param graphTracking source of data for population
	 */
	public static void populateTrackingRecord(DataRecord record, GraphTracking graphTracking) {
		populateGraph(graphTracking, record);
	}
	
	private static void populateGraph(GraphTracking graphTracking, DataRecord record) {
		populateField(record, composeFieldName(graphTracking, Attribute.START_TIME), graphTracking.getStartTime());
		populateField(record, composeFieldName(graphTracking, Attribute.END_TIME), graphTracking.getEndTime());
		populateField(record, composeFieldName(graphTracking, Attribute.EXECUTION_TIME), graphTracking.getExecutionTime());
		populateField(record, composeFieldName(graphTracking, Attribute.GRAPH_NAME), graphTracking.getGraphName());
		populateField(record, composeFieldName(graphTracking, Attribute.RESULT), graphTracking.getResult().message());
		PhaseTracking runningPhase = graphTracking.getRunningPhaseTracking();
		populateField(record, composeFieldName(graphTracking, Attribute.RUNNING_PHASE), ((runningPhase==null) ? null : runningPhase.getPhaseNum()) );
		populateField(record, composeFieldName(graphTracking, Attribute.USED_MEMORY), graphTracking.getUsedMemory());

		PhaseTracking[] phases = graphTracking.getPhaseTracking();
		if (phases != null) { //it is not probably necessary, just for sure
			for (PhaseTracking phase : phases) {
				populatePhase(phase, record);
			}
		}
	}
	
	private static void populatePhase(PhaseTracking phaseTracking, DataRecord record) {
		populateField(record, composeFieldName(phaseTracking, Attribute.START_TIME), phaseTracking.getStartTime());
		populateField(record, composeFieldName(phaseTracking, Attribute.END_TIME), phaseTracking.getEndTime());
		populateField(record, composeFieldName(phaseTracking, Attribute.EXECUTION_TIME), phaseTracking.getExecutionTime());
		populateField(record, composeFieldName(phaseTracking, Attribute.MEMORY_UTILIZATION), phaseTracking.getMemoryUtilization());
		populateField(record, composeFieldName(phaseTracking, Attribute.RESULT), phaseTracking.getResult().message());

		NodeTracking[] nodes = phaseTracking.getNodeTracking();
		if (nodes != null) {
			for (NodeTracking node : nodes) {
				populateNode(node, record);
			}
		}
	}
	
	private static void populateNode(NodeTracking nodeTracking, DataRecord record) {
		populateField(record, composeFieldName(nodeTracking, Attribute.NAME), nodeTracking.getNodeName());
		populateField(record, composeFieldName(nodeTracking, Attribute.USAGE_CPU), nodeTracking.getUsageCPU());
		populateField(record, composeFieldName(nodeTracking, Attribute.USAGE_USER), nodeTracking.getUsageUser());
		populateField(record, composeFieldName(nodeTracking, Attribute.PEAK_USAGE_CPU), nodeTracking.getPeakUsageCPU());
		populateField(record, composeFieldName(nodeTracking, Attribute.PEAK_USAGE_USER), nodeTracking.getPeakUsageUser());
		populateField(record, composeFieldName(nodeTracking, Attribute.TOTAL_CPU_TIME), nodeTracking.getTotalCPUTime());
		populateField(record, composeFieldName(nodeTracking, Attribute.TOTAL_USER_TIME), nodeTracking.getTotalUserTime());
		populateField(record, composeFieldName(nodeTracking, Attribute.RESULT), nodeTracking.getResult().message());
		populateField(record, composeFieldName(nodeTracking, Attribute.USED_MEMORY), nodeTracking.getUsedMemory());

		InputPortTracking[] inputPorts = nodeTracking.getInputPortTracking();
		if (inputPorts != null) {
			for (InputPortTracking port : inputPorts) {
				populatePort(port, record, true);
			}
		}
		
		OutputPortTracking[] outputPorts = nodeTracking.getOutputPortTracking();
		if (outputPorts != null) {
			for (OutputPortTracking port : outputPorts) {
				populatePort(port, record, false);
			}
		}
	}

	private static void populatePort(PortTracking portTracking, DataRecord record, boolean isInput) {
		populateField(record, composeFieldName(portTracking, Attribute.BYTE_FLOW, isInput), portTracking.getByteFlow());
		populateField(record, composeFieldName(portTracking, Attribute.BYTE_PEAK, isInput), portTracking.getBytePeak());
		populateField(record, composeFieldName(portTracking, Attribute.TOTAL_BYTES, isInput), portTracking.getTotalBytes());
		populateField(record, composeFieldName(portTracking, Attribute.RECORD_FLOW, isInput), portTracking.getRecordFlow());
		populateField(record, composeFieldName(portTracking, Attribute.RECORD_PEAK, isInput), portTracking.getRecordPeak());
		populateField(record, composeFieldName(portTracking, Attribute.TOTAL_RECORDS, isInput), portTracking.getTotalRecords());
		populateField(record, composeFieldName(portTracking, Attribute.WAITING_RECORDS, isInput), portTracking.getWaitingRecords());
		populateField(record, composeFieldName(portTracking, Attribute.AVERAGE_WAITING_RECORDS, isInput), portTracking.getAverageWaitingRecords());
		populateField(record, composeFieldName(portTracking, Attribute.USED_MEMORY, isInput), portTracking.getUsedMemory());
	}

	private static void populateField(DataRecord trackingRecord, String fieldName, Object value) {
		if (trackingRecord.hasField(fieldName)) {
			trackingRecord.getField(fieldName).setValue(value);
		}
	}
	
	//graph_{attr}
	private static String composeFieldName(GraphTracking graphTracking, Attribute attribute) {
		return FieldType.GRAPH + "_" + attribute;
	}
	
	//phase_{num}_{attr}
	private static String composeFieldName(PhaseTracking phaseTracking, Attribute attribute) {
		return FieldType.PHASE + "_" + phaseTracking.getPhaseNum() + "_" + attribute;
	}

	//component_{id}_{attr}
	private static String composeFieldName(NodeTracking nodeTracking, Attribute attribute) {
		return FieldType.COMPONENT + "_" + nodeTracking.getNodeID() + "_" + attribute;
	}

	//[inputPort|outputPort]_{port}_{componentId}_{attr}
	private static String composeFieldName(PortTracking portTracking, Attribute attribute, boolean isInput) {
		return (isInput ? FieldType.INPUT_PORT : FieldType.OUTPUT_PORT) + "_"
				+ portTracking.getIndex() + "_"
				+ portTracking.getParentNodeTracking().getNodeID() + "_"
				+ attribute;
	}

	/**
	 * Analyse given field name and returns respective field name structure representation if possible.
	 * Otherwise {@link JetelRuntimeException} is thrown.
	 */
	public static FieldNameStructure analyseFieldName(String fieldName) {
		for (FieldType fieldType : FieldType.values()) {
			FieldNameStructure fieldNameStructure = fieldType.tryToAnalyseFieldName(fieldName);
			if (fieldNameStructure != null) {
				return fieldNameStructure;
			}
		}
		
		throw new JetelRuntimeException("unsupported field name " + fieldName);
	}
	
	/**
	 * Field name structure representation. All field names generated by this toolkit in tracking metadata
	 * can be represented by this class.
	 */
	public static class FieldNameStructure {
		private FieldType fieldType;
		private Integer index;
		private String componentId;
		private Attribute attribute;
		
		private FieldNameStructure(FieldType fieldType, Integer index, String componentId, Attribute attribute) {
			this.fieldType = fieldType;
			this.index = index;
			this.componentId = componentId;
			this.attribute = attribute;
		}

		private static GraphFieldNameStructure createGraphFieldName(Attribute attribute) {
			return new GraphFieldNameStructure();
		}

		private static PhaseFieldNameStructure createPhaseFieldName(int index, Attribute attribute) {
			return new PhaseFieldNameStructure(index);
		}

		private static ComponentFieldNameStructure createComponentFieldName(String componentId, Attribute attribute) {
			return new ComponentFieldNameStructure(componentId, attribute);
		}

		private static PortFieldNameStructure createInputPortFieldName(int index, String componentId, Attribute attribute) {
			return new PortFieldNameStructure(FieldType.INPUT_PORT, index, componentId, attribute);
		}
		
		private static PortFieldNameStructure createOutputPortFieldName(int index, String componentId, Attribute attribute) {
			return new PortFieldNameStructure(FieldType.OUTPUT_PORT, index, componentId, attribute);
		}

		public FieldType getType() {
			return fieldType;
		}
		
		protected int getIndex() {
			if (index == null) {
				throw new UnsupportedOperationException();
			}
			return index;
		}
		
		protected String getComponentId() {
			if (componentId == null) {
				throw new UnsupportedOperationException();
			}
			return componentId;
		}

		protected Attribute getAttribute() {
			if (attribute == null) {
				throw new UnsupportedOperationException();
			}
			return attribute;
		}
	}

	public static class GraphFieldNameStructure extends FieldNameStructure {
		private GraphFieldNameStructure() {
			super(FieldType.GRAPH, null, null, null);
		}
	}

	public static class PhaseFieldNameStructure extends FieldNameStructure {
		private PhaseFieldNameStructure(int index) {
			super(FieldType.PHASE, index, null, null);
		}

		@Override
		public int getIndex() {
			return super.getIndex();
		}
	}

	public static class ComponentFieldNameStructure extends FieldNameStructure {
		private ComponentFieldNameStructure(String componentId, Attribute attribute) {
			super(FieldType.COMPONENT, null, componentId, attribute);
		}

		@Override
		public String getComponentId() {
			return super.getComponentId();
		}

		@Override
		public Attribute getAttribute() {
			return super.getAttribute();
		}
	}

	public static class PortFieldNameStructure extends FieldNameStructure {
		private PortFieldNameStructure(FieldType fieldType, int index, String componentId, Attribute attribute) {
			super(fieldType, index, componentId, attribute);
		}
		@Override
		public FieldType getType() {
			return super.getType();
		}

		@Override
		public int getIndex() {
			return super.getIndex();
		}

		@Override
		public String getComponentId() {
			return super.getComponentId();
		}

		@Override
		public Attribute getAttribute() {
			return super.getAttribute();
		}
	}

}

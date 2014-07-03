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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.jetel.component.RecordTransform;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordWithInvalidState;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.TransformException;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.DictionaryValuesContainer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.graph.runtime.IAuthorityProxy.RunStatus;
import org.jetel.graph.runtime.jmx.TrackingEvent;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.SandboxUrlUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.string.StringUtils;

/**
 * Jobflow related utilities.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30.5.2012
 */
public class JobflowUtils {

	public static final String PRODUCT_ID = "com.cloveretl.server";
	public static final String RUNTIME_PRODUCT_ID = "com.cloveretl.runtime";
	
	public static final String FEATURE_ID = "com.cloveretl.server.jobflow";
	
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
    private static final String RS_EXECUTION_LABEL_NAME = "executionLabel";
    public static final String RS_STATUS_NAME = "status";
    private static final String RS_ERR_EXCEPTION_NAME = "errException";
    private static final String RS_ERR_MESSAGE_NAME = "errMessage";
    private static final String RS_ERR_COMPONENT_NAME = "errComponent";
    private static final String RS_ERR_COMPONENT_TYPE_NAME = "errComponentType";
    
    public static final String PUBLIC_GRAPH_PARAMETER_ATTRIBUTE = "public";
    public static final String REQUIRED_GRAPH_PARAMETER_ATTRIBUTE = "required";

    /**
     * Static definition of default tracking event list for graph monitoring - list of "JMX" events
     * of executed graph which are listened. For now this list is statically defined since only "graphFinish"
     * is supported by clover server. It is possible to extend this functionality in the future.
     * Other possible tracking events could be "phaseFinished", "nodeFinished" or "trackingUpdate".
     * @see MonitorGraph
     */
    public static final List<TrackingEvent> DEFAULT_TRACKING_EVENT_LIST;
	static {
		DEFAULT_TRACKING_EVENT_LIST = new ArrayList<TrackingEvent>();
		DEFAULT_TRACKING_EVENT_LIST.add(TrackingEvent.GRAPH_FINISHED);
		DEFAULT_TRACKING_EVENT_LIST.add(TrackingEvent.JOBFLOW_FINISHED);
		DEFAULT_TRACKING_EVENT_LIST.add(TrackingEvent.PROFILER_JOB_FINISHED);
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
				String description = "Default value: '" + graphParameter.getValue() + "'";
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
	
}

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetel.component.RecordTransform;
import org.jetel.component.RecordTransformFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.MissingFieldException;
import org.jetel.exception.TransformException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.CloverClassPath;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.primitive.IdentityArrayList;
import org.jetel.util.string.StringUtils;

/**
 * This utility class wraps ordinary CTL transformation to make it more handy and useful mainly for various input/output mapping of jobflow
 * components. This class takes CTL transformation code, input records metadata and output records metadata. Optionally default output values
 * can be specified and star-map which is used in case CTL code is not specified.
 * 
 * In case CTL transformation is not specified, defined star-map is used instead.
 * 
 * All output data records in initialization time are preserved for future usage. These values are considered as default values
 * and all output records are populated by these values before each mapping execution. 
 * 
 * Expected method invocation order:
 *  - constructor
 *  - {@link #addInputMetadata(String, DataRecordMetadata)} or {@link #addInputRecord(String, DataRecord)}
 *  - {@link #addOutputMetadata(String, DataRecordMetadata)} or {@link #addOutputRecord(String, DataRecord)}
 *  - {@link #setDefaultOutputValue(String, String, Object)}
 *  - {@link #addAutoMapping(String, String)}
 *  - {@link #setTransformation(String)}
 *  - {@link #setClasspath(CloverClassPath)}
 *  - {@link #setClassLoader(ClassLoader)}
 *  - {@link #init()}
 *  - {@link #preExecute()}
 *  - {@link #execute()}
 *  - {@link #postExecute()}
 *  - {@link #free()}
 *   
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29.6.2012
 */
public class CTLMapping {

	/**
	 * name of mapping - used mainly for errors/warnings reporting
	 */
	private String name;
	
	/**
	 * associated component, needed for CTL code compilation
	 */
	private Node component;
	
	/**
	 * optional CTL code, alternatively can be substituted by auto-mappings (star-maps)
	 */
	private String sourceCode;
	
	/**
	 * list of data records pairs which represents auto-mapping (star-map) in case CTL code is not specified
	 */
	private List<DataRecord[]> autoMapping;
	
	/**
	 * classpath needed for CTL compilation
	 */
	private CloverClassPath classpath;
	
	/**
	 * classloader needed for CTL compilation
	 */
	private ClassLoader classLoader;
	
	/**
	 * compiled CTL code or null if CTL code is not specified
	 */
	private RecordTransform ctlTransformation;
	
	/**
	 * Map from name/identifier to input record - each input record has string identifier
	 */
	private Map<String, DataRecord> inputRecordsMap;
	
	/**
	 * Map from name/identifier to output record - each output record has string identifier
	 */
	private Map<String, DataRecord> outputRecordsMap;
	
	/**
	 * list of all input records - structure used only in pre-init time
	 */
	private List<DataRecord> inputRecordsList;
	
	/**
	 * list of all output records - structure used only in pre-init time
	 */
	private List<DataRecord> outputRecordsList;
	
	/**
	 * array of all input records - structure populated in init and used in execution time
	 */
	private DataRecord[] inputRecordsArray;
	
	/**
	 * array of all output records - structure populated in init and used in execution time
	 */
	private DataRecord[] outputRecordsArray;
	
	/**
	 * default values which are prepared in output records before each mapping execution
	 */
	private DataRecord[] defaultOutputRecords;
	
	/**
	 * metadata of all input records
	 */
	private DataRecordMetadata[] inputRecordsMetadata;

	/**
	 * metadata of all output records
	 */
	private DataRecordMetadata[] outputRecordsMetadata;

	/**
	 * is the CTL mapping already initialized?
	 */
	private boolean isInitialized = false;
	
	/**
	 * Only constructor
	 * @param name name of CTL mapping used mainly for reporting purposes
	 * @param component associated component used mainly for CTL mapping compilation
	 */
	public CTLMapping(String name, Node component) {
		this.name = name;
		this.component = component;
		
		autoMapping = new LinkedList<DataRecord[]>();
		
		inputRecordsMap = new HashMap<String, DataRecord>();
		outputRecordsMap = new HashMap<String, DataRecord>();
		
		inputRecordsList = new IdentityArrayList<DataRecord>();
		outputRecordsList = new IdentityArrayList<DataRecord>();
	}
	
	/**
	 * Adds new input metadata and respective data record is returned.
	 * @param name identifier of the input record
	 * @param inputMetadata new input metadata to be registered
	 * @return instance of given metadata which is used later for mapping execution
	 */
	public DataRecord addInputMetadata(String name, DataRecordMetadata inputMetadata) {
		assert (!StringUtils.isEmpty(name));
		
		if (inputMetadata != null) {
			DataRecord inputRecord = DataRecordFactory.newRecord(inputMetadata);
			inputRecord.init();
			inputRecord.reset();
			addInputRecord(name, inputRecord);
			return inputRecord;
		} else {
			addInputRecord(name, null);
			return null;
		}
		
	}
	
	/**
	 * Adds the given record into list of mapping's input records.
	 * @param name identifier of the input record
	 * @param inputRecord input record to be registered
	 */
	public void addInputRecord(String name, DataRecord inputRecord) {
		if (inputRecordsMap.containsKey(name)) {
			throw new IllegalArgumentException("input name already used");
		}
		inputRecordsMap.put(name, inputRecord);
		inputRecordsList.add(inputRecord);
	}
	
	/**
	 * Adds new output metadata and respective data record is returned.
	 * @param name identifier of the output record
	 * @param outputMetadata new output metadata to be registered
	 * @return instance of given metadata which is used later for mapping execution
	 */
	public DataRecord addOutputMetadata(String name, DataRecordMetadata outputMetadata) {
		if (outputMetadata != null) {
			DataRecord outputRecord = DataRecordFactory.newRecord(outputMetadata);
			outputRecord.init();
			outputRecord.reset();
			addOutputRecord(name, outputRecord);
			return outputRecord;
		} else {
			addOutputRecord(name, null);
			return null;
		}
	}
	
	/**
	 * Adds the given record into list of mapping's output records.
	 * @param name identifier of the output record
	 * @param outputRecord output record to be registered
	 */
	public void addOutputRecord(String name, DataRecord outputRecord) {
		if (outputRecordsMap.containsKey(name)) {
			throw new IllegalArgumentException("output name already used");
		}
		outputRecordsMap.put(name, outputRecord);
		outputRecordsList.add(outputRecord);
	}
	
	/**
	 * @param name identifier of requested input record
	 * @return input data record associated with the given identifier
	 */
	public DataRecord getInputRecord(String name) {
		if (!inputRecordsMap.containsKey(name)) {
			return null;
		}
		return inputRecordsMap.get(name);
	}

	/**
	 * @param name identifier of requested output record
	 * @return output data record associated with the given identifier
	 */
	public DataRecord getOutputRecord(String name) {
		if (!outputRecordsMap.containsKey(name)) {
			return null;
		}
		return outputRecordsMap.get(name);
	}

	/**
	 * @param index of requested input record
	 * @return input data record at the given index
	 */
	public DataRecord getInputRecord(int index) {
		if (index < 0 || index >= inputRecordsList.size()) {
			return null;
		}
		return inputRecordsList.get(index);
	}

	/**
	 * @param name index of requested output record
	 * @return output data record at the given index
	 */
	public DataRecord getOutputRecord(int index) {
		if (index < 0 || index >= outputRecordsList.size()) {
			return null;
		}
		return outputRecordsList.get(index);
	}

	/**
	 * @param name identifier of probed input record
	 * @return <code>true</code> if and only if the CTL mapping has input record associated with the given identifier
	 */
	public boolean hasInput(String name) {
		return inputRecordsMap.containsKey(name);
	}

	/**
	 * @param name identifier of probed output record
	 * @return <code>true</code> if and only if the CTL mapping has output record associated with the given identifier
	 */
	public boolean hasOutput(String name) {
		return outputRecordsMap.containsKey(name);
	}

	/**
	 * Sets a default output value. Needs to be called before {@link #init()} 
	 * @param outputName identifier of output record
	 * @param fieldName name of output field
	 * @param value default value
	 */
	public void setDefaultOutputValue(String outputName, String fieldName, Object value) {
		if (isInitialized) {
			throw new IllegalStateException("default cannot be changed after mapping initialization");
		}
		if (!outputRecordsMap.containsKey(outputName)) {
			throw new IllegalArgumentException(String.format("output name '%s' does not exist", outputName));
		}
		DataRecord outputRecord = outputRecordsMap.get(outputName);
		if (!outputRecord.hasField(fieldName)) {
			throw new IllegalArgumentException(String.format("output '%s' does not have field '%s'", outputName, fieldName));
		}
		try {
			outputRecord.getField(fieldName).setValue(value);
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("field '%s' in output '%s' cannot be initialized by value '%s'", fieldName, outputName, String.valueOf(value)));
		}
	}
	
	/**
	 * Sets CTL transformation backed by this mapping.
	 * @param sourceCode
	 */
	public void setTransformation(String sourceCode) {
		this.sourceCode = sourceCode;
	}
	
	/**
	 * Adds new star-mapping, which is used in case the CTL transformation is not defined.
	 * @param inputName identifier of source record for star-map
	 * @param outputName identifier of target record for start-map
	 */
	public void addAutoMapping(String inputName, String outputName) {
		if (!inputRecordsMap.containsKey(inputName)) {
			return;
		}
		if (!outputRecordsMap.containsKey(outputName)) {
			return;
		}
		DataRecord inputRecord = inputRecordsMap.get(inputName);
		DataRecord outputRecord = outputRecordsMap.get(outputName);
		if (inputRecord != null && outputRecord != null) {
			autoMapping.add(new DataRecord[] { inputRecordsMap.get(inputName), outputRecordsMap.get(outputName) });
		}
	}
	
	/**
	 * Sets classpath which is required for CTL code compilation.
	 */
	public void setClasspath(CloverClassPath classpath) {
		this.classpath = classpath;
	}
	
	/**
	 * Sets classloader which is required for CTL code compilation.
	 * @param classLoader
	 */
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
	
	/**
	 * CTL mapping initialization can be called only once. Output records content at the time is considered as default output.
	 */
	public void init() throws ComponentNotReadyException {
		assert (!isInitialized);
		isInitialized = true;
		
		inputRecordsArray = new DataRecord[inputRecordsList.size()];
		inputRecordsArray = inputRecordsList.toArray(inputRecordsArray);

		outputRecordsArray = new DataRecord[outputRecordsList.size()];
		outputRecordsArray = outputRecordsList.toArray(outputRecordsArray);

		defaultOutputRecords = new DataRecord[outputRecordsArray.length];
		for (int i = 0; i < outputRecordsArray.length; i++) {
			if (outputRecordsArray[i] != null) {
				defaultOutputRecords[i] = outputRecordsArray[i].duplicate();
			}
		}
		
		inputRecordsMetadata = MiscUtils.extractMetadata(inputRecordsArray);
		outputRecordsMetadata = MiscUtils.extractMetadata(outputRecordsArray);
		
		//create CTL transformation
        if (!StringUtils.isEmpty(sourceCode)) {
			try {
				ctlTransformation = RecordTransformFactory.createTransform(sourceCode, null, 
						null, null, component, inputRecordsMetadata, outputRecordsMetadata,
						classLoader, classpath);
			} catch (MissingFieldException missingField) {
				throw missingField;
			} catch (Exception e) {
				throw new JetelRuntimeException(name + " is invalid.", e);
			}
			try {
				// initialise mapping
		        if (!ctlTransformation.init(null, inputRecordsMetadata, outputRecordsMetadata)) {
		            throw new ComponentNotReadyException(name + " initialization was unsuccessful.");
		        }
			} catch (Exception e) {
				throw new JetelRuntimeException(name + " initialization failed.", e);
			}
        }
	}
	
	/**
	 * This method should be invoked in preExecute phase of component life cycle.
	 */
	public void preExecute() {
		if (ctlTransformation != null) {
			try {
				ctlTransformation.preExecute();
			} catch (ComponentNotReadyException e) {
				throw new JetelRuntimeException(name + " pre-execution failed.");
			}
		}
	}
	
	/**
	 * All input records are reseted by this method. 
	 */
	public void resetInputRecords() {
		MiscUtils.resetRecords(inputRecordsArray);
	}
	
	/**
	 * Access to default output record, which is available after mapping initialization.
	 * @param name requested output record name
	 * @return requested default output record
	 */
	public DataRecord getDefaultOutputRecord(String name) {
		if (!isInitialized) {
			throw new IllegalStateException("default output record is available after initialization");
		}
		int index = indexOfOutputRecord(name);
		if (index != -1) {
			return defaultOutputRecords[index];
		} else {
			return null;
		}
	}
	
	/**
	 * Performs the mapping. Default values are copied into output records and either ctl transformation or simple star-mapping is performed.
	 * @return result of CTL transformation (see {@link RecordTransform#}) or {@link RecordTransform#ALL} for star-mapping is returned
	 */
	public int execute() {
		if (!isInitialized) {
			throw new IllegalStateException("mapping needs to be initialized before execution");
		}
		for (int i = 0; i < outputRecordsArray.length; i++) {
			if (outputRecordsArray[i] != null) {
				outputRecordsArray[i].copyFrom(defaultOutputRecords[i]);
			}
		}

		if (ctlTransformation != null) {
			try {
				return ctlTransformation.transform(inputRecordsArray, outputRecordsArray);
			} catch (Exception exception) {
				try {
					return ctlTransformation.transformOnError(exception, inputRecordsArray, outputRecordsArray);
				} catch (TransformException e) {
					throw new JetelRuntimeException(name + " transformation failed.", e);
				}
			}
		} else {
			for (DataRecord[] map : autoMapping) {
				map[1].copyFieldsByName(map[0]);
			}
			return RecordTransform.ALL;
		}
	}
	
	/**
	 * This method checks whether the specified output field was overridden by a 'user-value' in last {@link #execute()} method invocation.
	 * @param record output record of interest
	 * @param field output field of interest (needs to be field of @param record)
	 * @return
	 */
	public boolean isOutputOverridden(DataRecord record, DataField field) {
		if (record.getField(field.getMetadata().getName()) != field) {
			throw new IllegalArgumentException("field is not part of record");
		}
		Object newValue = field.getValue();
		int recordIndex = outputRecordsList.indexOf(record);
		if (recordIndex != -1) {
			Object defaultValue = defaultOutputRecords[recordIndex].getField(field.getMetadata().getName()).getValue();
			return !CompareUtils.equals(defaultValue, newValue);
		} else {
			throw new IllegalArgumentException("given record is not output record");
		}
	}

	/**
	 * Access method to output values
	 * @param name identifier of requested output record
	 * @param fieldName field name of requestede output field
	 * @return
	 */
	public Object getOutput(String name, String fieldName) {
		if (!outputRecordsMap.containsKey(name)) {
			throw new IllegalArgumentException(String.format("output name '%s' does not exist", name));
		}
		DataRecord outputRecord = outputRecordsMap.get(name);
		
		if (!outputRecord.hasField(fieldName)) {
			throw new IllegalArgumentException(String.format("output name '%s' does not have field '%s'", name, fieldName));
		}
		
		return outputRecord.getField(fieldName).getValue();
	}
	
	/**
	 * This method should be invoked in preExecute phase of component life cycle.
	 */
	public void postExecute() {
		if (ctlTransformation != null) {
			try {
				ctlTransformation.postExecute();
			} catch (ComponentNotReadyException e) {
				throw new JetelRuntimeException(name + " post-execution failed.");
			}
		}
	}

	private int indexOfOutputRecord(String name) {
		DataRecord outputRecord = outputRecordsMap.get(name);
		if (outputRecord != null) {
			return outputRecordsList.indexOf(outputRecord);
		} else {
			return -1;
		}
	}

	private int indexOfInputRecord(String name) {
		DataRecord inputRecord = inputRecordsMap.get(name);
		if (inputRecord != null) {
			return inputRecordsList.indexOf(inputRecord);
		} else {
			return -1;
		}
	}

	public List<DataFieldMetadata> findUsedInputFields(TransformationGraph graph) {
		if (!StringUtils.isEmpty(sourceCode)) {
			return CTLTransformUtils.findUsedInputFields(graph, inputRecordsMetadata, outputRecordsMetadata, sourceCode);
		} else {
			List<DataFieldMetadata> autoMapped = new ArrayList<DataFieldMetadata>();
			Set<String> outputMetadataNames = new HashSet<String>();
			for (DataRecord [] autoMappedPair : autoMapping) {
				DataRecordMetadata inputRecordMetadata = autoMappedPair[0].getMetadata();  //for some reason, Kokon chose using array for pairs :-/
				DataRecordMetadata outputRecordMetadata = autoMappedPair[1].getMetadata();
				if (inputRecordMetadata!=null && outputRecordMetadata!=null) {
					for (DataFieldMetadata outputFieldMetadata : outputRecordMetadata.getFields()) {
						outputMetadataNames.add(outputFieldMetadata.getName());
					}
					for (DataFieldMetadata inputFieldMetadata : inputRecordMetadata.getFields()) {
						if (outputMetadataNames.contains(inputFieldMetadata.getName())) {
							autoMapped.add(inputFieldMetadata);
						}
					}
				}
			}
			return autoMapped;
		}
	}

	
}

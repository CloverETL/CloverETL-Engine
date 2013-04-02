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
package org.jetel.component;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.jetel.component.rollup.RecordRollup;
import org.jetel.component.rollup.RecordRollupDescriptor;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.DoubleRecordBuffer;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>Rollup Component</h3>
 *
 * <p>Executes the rollup transform on all the input data records.</p>
 *
 * <h4>Component:</h4>
 * <table border="1">
 * <tr>
 *   <td><b>Name:</b></td>
 *   <td>Rollup</td>
 * </tr>
 * <tr>
 *   <td><b>Category:</b></td>
 *   <td>transformers</td>
 * </tr>
 * <tr>
 *   <td><b>Description:</b></td>
 *   <td>
 *     A general transformation component that serves as an executor of rollup transforms written in Java or CTL.
 *     See the {@link RecordRollup} interface for more details on the life cycle of the rollup transform.
 *   </td>
 * </tr>
 * <tr>
 *   <td><b>Inputs:</b></td>
 *   <td>[0] - input data records</td>
 * </tr>
 * <tr>
 *   <td><b>Outputs:</b></td>
 *   <td>At least one connected output port.</td>
 * </tr>
 * </table>
 * 
 * <h4>XML attributes:</h4>
 * <table border="1">
 * <tr>
 *   <td><b>id</b></td>
 *   <td>ID of the component.</td>
 * </tr>
 * <tr>
 *   <td><b>type</b></td>
 *   <td>ROLLUP</td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>groupKeyFields</b>
 *     <i>optional</i>
 *   </td>
 *   <td>Field names that form the group key separated by ':', ';' or '|'.</td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>groupAccumulatorMetadataId</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>ID of data record metadata that should be used to create group "accumulators" (if required).</td>
 * </tr>
 * <tr>
 *   <td><b>transform</b></td>
 *   <td>Rollup transform as a Java or CTL source code.</td>
 * </tr>
 * <tr>
 *   <td><b>transformUrl</b></td>
 *   <td>URL of an external Java/CTL source code of the rollup transform.</td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>transformUrlCharset</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>Character set used in the source code of the rollup transform specified via an URL.</td>
 * </tr>
 * <tr>
 *   <td><b>transformClassName</b></td>
 *   <td>Class name of a Java class implementing the rollup transform.</td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>inputSorted</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>
 *      Flag specifying whether the input data records are sorted or not. If set to false, the order of output
 *      data records is not specified.
 *   </td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>equalNULL</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>Flag specifying whether the null values are considered equal or not.</td>
 * </tr>
 * </table>
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 22nd June 2010
 * @created 30th April 2009
 *
 * @see RecordRollup
 */
public class Rollup extends Node {

    /** the type of the component */
    private static final String COMPONENT_TYPE = "ROLLUP";

    //
    // names of XML attributes
    //

    /** the name of an XML attribute used to store the group key fields */
    public static final String XML_GROUP_KEY_FIELDS_ATTRIBUTE = "groupKeyFields";
    /** the name of an XML attribute used to store the ID of the group "accumulator" metadata */
    public static final String XML_GROUP_ACCUMULATOR_METADATA_ID_ATTRIBUTE = "groupAccumulatorMetadataId";

    /** the name of an XML attribute used to store the source code of a Java/CTL transform */
    public static final String XML_TRANSFORM_ATTRIBUTE = "transform";
    /** the name of an XML attribute used to store the URL of an external Java/CTL transform */
    public static final String XML_TRANSFORM_URL_ATTRIBUTE = "transformUrl";
    /** the name of an XML attribute used to store the character set used in the rollup transform specified via an URL */
    public static final String XML_TRANSFORM_URL_CHARSET_ATTRIBUTE = "transformUrlCharset";
    /** the name of an XML attribute used to store the class name of a Java transform */
    public static final String XML_TRANSFORM_CLASS_NAME_ATTRIBUTE = "transformClassName";

    /** the name of an XML attribute used to store the "input sorted" flag */
    public static final String XML_INPUT_SORTED_ATTRIBUTE = "inputSorted";
    /** the name of an XML attribute used to store the "equal NULL" flag */
    public static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";

    //
    // constants used during execution
    //

    /** the port index used for data record input */
    private static final int INPUT_PORT_NUMBER = 0;

    /**
     * Creates an instance of the <code>Rollup</code> component from an XML element.
     *
     * @param transformationGraph the transformation graph the component belongs to
     * @param xmlElement the XML element that should be used for construction
     *
     * @return an instance of the <code>Rollup</code> component
     *
     * @throws XMLConfigurationException when some attribute is missing
     * @throws AttributeNotFoundException 
     */
    public static Node fromXML(TransformationGraph transformationGraph, Element xmlElement)
            throws XMLConfigurationException, AttributeNotFoundException {
        Rollup rollup = null;

        ComponentXMLAttributes componentAttributes = new ComponentXMLAttributes(xmlElement, transformationGraph);

        if (!componentAttributes.getString(XML_TYPE_ATTRIBUTE).equalsIgnoreCase(COMPONENT_TYPE)) {
            throw new XMLConfigurationException("The " + StringUtils.quote(XML_TYPE_ATTRIBUTE)
                    + " attribute contains a value incompatible with this component!");
        }

        rollup = new Rollup(componentAttributes.getString(XML_ID_ATTRIBUTE));

        String groupKeyString = componentAttributes.getString(XML_GROUP_KEY_FIELDS_ATTRIBUTE, null);
        rollup.setGroupKeyFields(!StringUtils.isEmpty(groupKeyString)
                ? groupKeyString.trim().split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX) : null);
        rollup.setGroupAccumulatorMetadataId(
                componentAttributes.getString(XML_GROUP_ACCUMULATOR_METADATA_ID_ATTRIBUTE, null));

        rollup.setTransform(componentAttributes.getStringEx(XML_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
        rollup.setTransformUrl(componentAttributes.getStringEx(XML_TRANSFORM_URL_ATTRIBUTE, null,
        		RefResFlag.SPEC_CHARACTERS_OFF));
        rollup.setTransformUrlCharset(componentAttributes.getString(XML_TRANSFORM_URL_CHARSET_ATTRIBUTE, null));
        rollup.setTransformClassName(componentAttributes.getString(XML_TRANSFORM_CLASS_NAME_ATTRIBUTE, null));

        rollup.setTransformParameters(componentAttributes.attributes2Properties(new String[] {
        		XML_TYPE_ATTRIBUTE, XML_ID_ATTRIBUTE, XML_GROUP_KEY_FIELDS_ATTRIBUTE,
        		XML_GROUP_ACCUMULATOR_METADATA_ID_ATTRIBUTE, XML_TRANSFORM_ATTRIBUTE, XML_TRANSFORM_URL_ATTRIBUTE,
        		XML_TRANSFORM_URL_CHARSET_ATTRIBUTE, XML_TRANSFORM_CLASS_NAME_ATTRIBUTE, XML_INPUT_SORTED_ATTRIBUTE,
        		XML_EQUAL_NULL_ATTRIBUTE }));

        rollup.setInputSorted(componentAttributes.getBoolean(XML_INPUT_SORTED_ATTRIBUTE, true));
        rollup.setEqualNULL(componentAttributes.getBoolean(XML_EQUAL_NULL_ATTRIBUTE, true));

        return rollup;
    }

    //
    // component attributes read from or written to an XML document
    //

    /** the key fields specifying a key used to separate data records into groups */
    private String[] groupKeyFields;
    /** the ID of data record meta to be used for the group "accumulator" */
    private String groupAccumulatorMetadataId;

    /** the source code of a Java/CTL rollup transform */
    private String transform;
    /** the URL of an external Java/CTL rollup transform */
    private String transformUrl;
    /** the character set used in the rollup transform specified via an URL */
    private String transformUrlCharset;
    /** the class name of a Java rollup transform */
    private String transformClassName;

    /** the parameters passed to the init() method of the rollup transform */
	private Properties transformParameters;

	/** the flag specifying whether the input data records are sorted or not */
    private boolean inputSorted = true;
    /** the flag specifying whether the null values are considered equal or not */
    private boolean equalNULL = true;

    //
    // runtime attributes initialized in the init() method
    //

    /** the group key used to separate data records into groups */
    private RecordKey groupKey;
    /** an instance of the rollup transform used during the execution */ 
    private RecordRollup recordRollup;
    /** the data records used for output */
    private DataRecord[] outputRecords;

    /**
     * Constructs an instance of the <code>Rollup</code> component with the given ID.
     *
     * @param id an ID of the component
     */
    public Rollup(String id) {
        super(id);
    }
    
    public Rollup(String id, RecordRollup recordRollup) {
        this(id);
        this.recordRollup = recordRollup;
    }

    @Override
    public String getType() {
        return COMPONENT_TYPE;
    }

    public void setGroupKeyFields(String[] groupKeyFields) {
        this.groupKeyFields = groupKeyFields;
    }

    public void setGroupAccumulatorMetadataId(String groupAccumulatorMetadataId) {
        this.groupAccumulatorMetadataId = groupAccumulatorMetadataId;
    }

    public void setTransform(String transform) {
        this.transform = transform;
    }

    public void setTransformUrl(String transformUrl) {
        this.transformUrl = transformUrl;
    }

    public void setTransformUrlCharset(String transformUrlCharset) {
        this.transformUrlCharset = transformUrlCharset;
    }

    public void setTransformClassName(String transformClassName) {
        this.transformClassName = transformClassName;
    }

    public void setTransformParameters(Properties transformParameters) {
		this.transformParameters = transformParameters;
	}

    public void setInputSorted(boolean inputSorted) {
        this.inputSorted = inputSorted;
    }

    public void setEqualNULL(boolean equalNULL) {
        this.equalNULL = equalNULL;
    }

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        if (!checkInputPorts(status, 1, 1) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
            return status;
        }

        if (groupKeyFields != null && groupKeyFields.length != 0) {
            DataRecordMetadata metadata = getInputPort(INPUT_PORT_NUMBER).getMetadata();

            for (String groupKeyField : groupKeyFields) {
                if (metadata.getField(groupKeyField) == null) {
                    status.add(new ConfigurationProblem("The group key field " + StringUtils.quote(groupKeyField)
                            + " doesn't exist!", Severity.ERROR, this, Priority.HIGH, XML_GROUP_KEY_FIELDS_ATTRIBUTE));
                }
            }
        }

        if (groupAccumulatorMetadataId != null && getGraph().getDataRecordMetadata(groupAccumulatorMetadataId, false) == null) {
            status.add(new ConfigurationProblem("The group \"accumulator\" metadata ID is not valid!",
                    Severity.ERROR, this, Priority.HIGH, XML_GROUP_ACCUMULATOR_METADATA_ID_ATTRIBUTE));
        }

        if (StringUtils.isEmpty(transform) && StringUtils.isEmpty(transformUrl)
        		&& StringUtils.isEmpty(transformClassName)) {
            status.add(new ConfigurationProblem("No rollup transform specified!", Severity.ERROR, this, Priority.HIGH));
        }

        if (transformUrlCharset != null && !Charset.isSupported(transformUrlCharset)) {
            status.add(new ConfigurationProblem("The transform URL character set is not supported!",
                    Severity.ERROR, this, Priority.NORMAL, XML_TRANSFORM_URL_CHARSET_ATTRIBUTE));
        }
        
        //check transformation
        if (recordRollup == null) {
        	getTransformFactory().checkConfig(status);
        }

        return status;
    }

    @Override
    public synchronized void init() throws ComponentNotReadyException {
        if (isInitialized()) {
            throw new IllegalStateException("The component has already been initialized!");
        }

        super.init();

        if (groupKeyFields != null && groupKeyFields.length != 0) {
            groupKey = new RecordKey(groupKeyFields, getInputPort(INPUT_PORT_NUMBER).getMetadata());
            groupKey.setEqualNULLs(equalNULL);
            groupKey.init();
        }

        if (recordRollup == null) {
        	recordRollup = getTransformFactory().createTransform();
        }

        recordRollup.init(transformParameters, getInputPort(INPUT_PORT_NUMBER).getMetadata(),
                getGraph().getDataRecordMetadata(groupAccumulatorMetadataId),
                getOutMetadata().toArray(new DataRecordMetadata[getOutPorts().size()]));

        outputRecords = new DataRecord[getOutPorts().size()];

        for (int i = 0; i < outputRecords.length; i++) {
            outputRecords[i] = DataRecordFactory.newRecord(getOutputPort(i).getMetadata());
            outputRecords[i].init();
        }
    }

	private TransformFactory<RecordRollup> getTransformFactory() {
    	TransformFactory<RecordRollup> transformFactory = TransformFactory.createTransformFactory(RecordRollupDescriptor.newInstance());
    	transformFactory.setTransform(transform);
    	transformFactory.setTransformClass(transformClassName);
    	transformFactory.setTransformUrl(transformUrl);
    	transformFactory.setCharset(transformUrlCharset);
    	transformFactory.setComponent(this);
    	transformFactory.setInMetadata(getInMetadata());
    	transformFactory.setOutMetadata(getOutMetadata());
    	return transformFactory;
	}

    @Override
    @SuppressWarnings("deprecation")
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();

    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	} else {
    	    recordRollup.reset();
    	}
    }    

    @Override
    public Result execute() throws Exception {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        if (inputSorted || groupKey == null) {
            executeInputSorted();
        } else {
            executeInputUnsorted();
        }

        broadcastEOF();

        return (runIt ? Result.FINISHED_OK : Result.ABORTED);
    }

    /**
     * Execution code specific for sorted input of data records.
     *
     * @throws IOException if an error occurred while reading or writing data records
     * @throws InterruptedException if an error occurred while reading or writing data records
     * @throws JetelException if input data records are not sorted or an error occurred during the transformation
     */
    private void executeInputSorted() throws IOException, InterruptedException, JetelException {
        InputPort inputPort = getInputPort(INPUT_PORT_NUMBER);

        DoubleRecordBuffer inputRecords = new DoubleRecordBuffer(inputPort.getMetadata());
        DataRecord groupAccumulator = null;

        if (groupAccumulatorMetadataId != null) {
            groupAccumulator = DataRecordFactory.newRecord(getGraph().getDataRecordMetadata(groupAccumulatorMetadataId));
            groupAccumulator.init();
            groupAccumulator.reset();
        }

        if (inputPort.readRecord(inputRecords.getCurrent()) != null) {
        	try {
        		recordRollup.initGroup(inputRecords.getCurrent(), groupAccumulator);
        	} catch (Exception exception) {
        		recordRollup.initGroupOnError(exception, inputRecords.getCurrent(), groupAccumulator);
			}

        	boolean updateGroupResult = false;

        	try {
        		updateGroupResult = recordRollup.updateGroup(inputRecords.getCurrent(), groupAccumulator);
        	} catch (Exception exception) {
        		updateGroupResult = recordRollup.updateGroupOnError(exception,
        				inputRecords.getCurrent(), groupAccumulator);
			}

        	if (updateGroupResult) {
                updateTransform(inputRecords.getCurrent(), groupAccumulator);
            }

            inputRecords.swap();

            int sortDirection = 0;

            while (runIt && inputPort.readRecord(inputRecords.getCurrent()) != null) {
                int comparisonResult = (groupKey != null) ? groupKey.compare(
                        inputRecords.getCurrent(), inputRecords.getPrevious()) : 0;

                if (comparisonResult != 0) {
                    if (sortDirection == 0) {
                        sortDirection = comparisonResult;
                    } else if (comparisonResult != sortDirection) {
                        throw new JetelException("Input data records not sorted!");
                    }

                    boolean finishGroupResult = false;

                	try {
                		finishGroupResult = recordRollup.finishGroup(inputRecords.getPrevious(), groupAccumulator);
                	} catch (Exception exception) {
                		finishGroupResult = recordRollup.finishGroupOnError(exception,
                				inputRecords.getPrevious(), groupAccumulator);
        			}

                    if (finishGroupResult) {
                        transform(inputRecords.getPrevious(), groupAccumulator);
                    }

                    if (groupAccumulator != null) {
                        groupAccumulator.reset();
                    }

                	try {
                		recordRollup.initGroup(inputRecords.getCurrent(), groupAccumulator);
                	} catch (Exception exception) {
                		recordRollup.initGroupOnError(exception, inputRecords.getCurrent(), groupAccumulator);
        			}
                }

            	try {
            		updateGroupResult = recordRollup.updateGroup(inputRecords.getCurrent(), groupAccumulator);
            	} catch (Exception exception) {
            		updateGroupResult = recordRollup.updateGroupOnError(exception,
            				inputRecords.getCurrent(), groupAccumulator);
    			}

                if (updateGroupResult) {
                    updateTransform(inputRecords.getCurrent(), groupAccumulator);
                }

                inputRecords.swap();
                SynchronizeUtils.cloverYield();
            }

            boolean finishGroupResult = false;

        	try {
        		finishGroupResult = recordRollup.finishGroup(inputRecords.getPrevious(), groupAccumulator);
        	} catch (Exception exception) {
        		finishGroupResult = recordRollup.finishGroupOnError(exception,
        				inputRecords.getPrevious(), groupAccumulator);
			}

            if (finishGroupResult) {
                transform(inputRecords.getPrevious(), groupAccumulator);
            }
        }
    }

    /**
     * Execution code specific for unsorted input of data records.
     *
     * @throws TransformException if an error occurred during the transformation
     * @throws IOException if an error occurred while reading or writing data records
     * @throws InterruptedException if an error occurred while reading or writing data records
     */
    private void executeInputUnsorted() throws TransformException, IOException, InterruptedException {
        InputPort inputPort = getInputPort(INPUT_PORT_NUMBER);

        DataRecord inputRecord = DataRecordFactory.newRecord(inputPort.getMetadata());
        inputRecord.init();

        DataRecordMetadata groupAccumulatorMetadata = (groupAccumulatorMetadataId != null)
                ? getGraph().getDataRecordMetadata(groupAccumulatorMetadataId) : null;
        Map<HashKey, DataRecord> groupAccumulators = new LinkedHashMap<HashKey, DataRecord>();

        HashKey lookupKey = new HashKey(groupKey, inputRecord);

        while (runIt && inputPort.readRecord(inputRecord) != null) {
            DataRecord groupAccumulator = groupAccumulators.get(lookupKey);

            if (groupAccumulator == null && !groupAccumulators.containsKey(lookupKey)) {
                if (groupAccumulatorMetadata != null) {
                    groupAccumulator = DataRecordFactory.newRecord(groupAccumulatorMetadata);
                    groupAccumulator.init();
                    groupAccumulator.reset();
                }

                groupAccumulators.put(new HashKey(groupKey, inputRecord.duplicate()), groupAccumulator);

                try {
                	recordRollup.initGroup(inputRecord, groupAccumulator);
                } catch (Exception exception) {
                	recordRollup.initGroupOnError(exception, inputRecord, groupAccumulator);
				}
            }

            boolean updateGroupResult = false;

            try {
            	updateGroupResult = recordRollup.updateGroup(inputRecord, groupAccumulator);
            } catch (Exception exception) {
            	updateGroupResult = recordRollup.updateGroupOnError(exception, inputRecord, groupAccumulator);
			}

            if (updateGroupResult) {
                updateTransform(inputRecord, groupAccumulator);
            }

            SynchronizeUtils.cloverYield();
        }

        Iterator<Map.Entry<HashKey, DataRecord>> groupAccumulatorsIterator = groupAccumulators.entrySet().iterator();

        while (runIt && groupAccumulatorsIterator.hasNext()) {
            Map.Entry<HashKey, DataRecord> entry = groupAccumulatorsIterator.next();

            boolean finishGroupResult = false;

            try {
            	finishGroupResult = recordRollup.finishGroup(entry.getKey().getDataRecord(), entry.getValue());
            } catch (Exception exception) {
            	finishGroupResult = recordRollup.finishGroupOnError(exception,
            			entry.getKey().getDataRecord(), entry.getValue());
			}

            if (finishGroupResult) {
                transform(entry.getKey().getDataRecord(), entry.getValue());
            }
        }
    }

    /**
     * Calls the updateTransform() method on the rollup transform and performs further processing based on the result.
     *
     * @param inputRecord the current input data record
     * @param groupAccumulator the group "accumulator" for the current group
     *
     * @throws TransformException if an error occurred during the transformation
     * @throws IOException if an error occurred while writing a data record to an output port
     * @throws InterruptedException if an error occurred while writing a data record to an output port
     */
    private void updateTransform(DataRecord inputRecord, DataRecord groupAccumulator)
            throws TransformException, IOException, InterruptedException {
        int counter = 0;

        while (true) {
            for (DataRecord outputRecord : outputRecords) {
                outputRecord.reset();
            }

            int transformResult = -1; 

            try {
            	transformResult = recordRollup.updateTransform(counter, inputRecord, groupAccumulator, outputRecords);
            } catch (Exception exception) {
            	transformResult = recordRollup.updateTransformOnError(exception, counter,
            			inputRecord, groupAccumulator, outputRecords);
			}

            counter++;

            if (transformResult == RecordRollup.SKIP) {
                break;
            }

            if (transformResult == RecordRollup.ALL) {
                for (int i = 0; i < outputRecords.length; i++) {
                    writeRecord(i, outputRecords[i]);
                }
            } else if (transformResult >= 0) {
                writeRecord(transformResult, outputRecords[transformResult]);
            } else {
                throw new TransformException("Transformation finished with error " + transformResult + ": "
                        + recordRollup.getMessage());
            }
        }
    }

    /**
     * Calls the transform() method on the rollup transform and performs further processing based on the result.
     *
     * @param inputRecord the current input data record
     * @param groupAccumulator the group "accumulator" for the current group
     *
     * @throws TransformException if an error occurred during the transformation
     * @throws IOException if an error occurred while writing a data record to an output port
     * @throws InterruptedException if an error occurred while writing a data record to an output port
     */
    private void transform(DataRecord inputRecord, DataRecord groupAccumulator)
            throws TransformException, IOException, InterruptedException {
        int counter = 0;

        while (true) {
            for (DataRecord outputRecord : outputRecords) {
                outputRecord.reset();
            }

            int transformResult = -1;

            try {
            	transformResult = recordRollup.transform(counter, inputRecord, groupAccumulator, outputRecords);
            } catch (Exception exception) {
            	transformResult = recordRollup.transformOnError(exception, counter,
            			inputRecord, groupAccumulator, outputRecords);
			}

            counter++;

            if (transformResult == RecordRollup.SKIP) {
                break;
            }

            if (transformResult == RecordRollup.ALL) {
                for (int i = 0; i < outputRecords.length; i++) {
                    writeRecord(i, outputRecords[i]);
                }
            } else if (transformResult >= 0) {
                writeRecord(transformResult, outputRecords[transformResult]);
            } else {
                throw new TransformException("Transformation finished with error " + transformResult + ": "
                        + recordRollup.getMessage());
            }
        }
    }

	@Override
    @SuppressWarnings("deprecation")
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();

    	recordRollup.postExecute();
    	recordRollup.finished();
    }

    @Override
    public synchronized void free() {
        groupKey = null;
        recordRollup = null;
        outputRecords = null;

        super.free();
    }

}

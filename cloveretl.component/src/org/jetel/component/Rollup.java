/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2009  David Pavlis <david.pavlis@javlin.eu>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.component;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.jetel.component.rollup.RecordRollup;
import org.jetel.component.rollup.RecordRollupTL;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.DoubleRecordBuffer;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
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
 *     Serves as an executor of rollup transforms written in Java or CTL. See the {@link RecordRollup} interface
 *     for more details on the life cycle of the rollup transform.
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
 *   <td>An ID of the component.</td>
 * </tr>
 * <tr>
 *   <td><b>type</b></td>
 *   <td>ROLLUP</td>
 * </tr>
 * <tr>
 *   <td><b>groupKeyFields</b></td>
 *   <td>Field names that form the group key separated by ':', ';' or '|'.</td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>groupAccumulatorMetadataId</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>An ID of data record meta data that should be used to create group "accumulators" (if required).</td>
 * </tr>
 * <tr>
 *   <td><b>transform</b></td>
 *   <td>A rollup transform as a Java or CTL source code.</td>
 * </tr>
 * <tr>
 *   <td><b>transformUrl</b></td>
 *   <td>A URL of an external Java/CTL source code of the rollup transform.</td>
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
 *   <td>A class name of a Java class implementing the rollup transform.</td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>inputSorted</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>A flag specifying whether the input data records are sorted or not.</td>
 * </tr>
 * </table>
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 30th April 2009
 * @since 30th April 2009
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
    private static final String XML_GROUP_KEY_FIELDS_ATTRIBUTE = "groupKeyFields";
    /** the name of an XML attribute used to store the ID of the group "accumulator" meta data */
    private static final String XML_GROUP_ACCUMULATOR_METADATA_ID_ATTRIBUTE = "groupAccumulatorMetadataId";

    /** the name of an XML attribute used to store the source code of a Java/CTL transform */
    private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
    /** the name of an XML attribute used to store the URL of an external Java/CTL transform */
    private static final String XML_TRANSFORM_URL_ATTRIBUTE = "transformUrl";
    /** the name of an XML attribute used to store the character set used in the rollup transform specified via an URL */
    private static final String XML_TRANSFORM_URL_CHARSET_ATTRIBUTE = "transformUrlCharset";
    /** the name of an XML attribute used to store the class name of a Java transform */
    private static final String XML_TRANSFORM_CLASS_NAME_ATTRIBUTE = "transformClassName";

    /** the name of an XML attribute used to store the "input sorted" flag */
    private static final String XML_INPUT_SORTED_ATTRIBUTE = "inputSorted";

    //
    // constants used during execution
    //

    /** the port index used for data record input */
    private static final int INPUT_PORT_NUMBER = 0;

    /** the regular expression pattern that should be present in a Java source code transformation */
    private static final String REGEX_JAVA_CLASS = "class\\s+\\w+";
    /** the regular expression pattern that should be present in a CTL code transformation */
    private static final String REGEX_TL_CODE = "function\\s+((init|update|finish)Group|transform)";

    /**
     * Creates an instance of the <code>Rollup</code> component from an XML element.
     *
     * @param transformationGraph the transformation graph the component belongs to
     * @param xmlElement the XML element that should be used for construction
     *
     * @return an instance of the <code>Rollup</code> component
     *
     * @throws XMLConfigurationException when some attribute is missing
     */
    public static Node fromXML(TransformationGraph transformationGraph, Element xmlElement)
            throws XMLConfigurationException {
        Rollup rollup = null;

        ComponentXMLAttributes componentAttributes = new ComponentXMLAttributes(xmlElement, transformationGraph);

        try {
            if (!componentAttributes.getString(XML_TYPE_ATTRIBUTE).equalsIgnoreCase(COMPONENT_TYPE)) {
                throw new XMLConfigurationException("The " + StringUtils.quote(XML_TYPE_ATTRIBUTE)
                        + " attribute contains a value incompatible with this component!");
            }

            rollup = new Rollup(componentAttributes.getString(XML_ID_ATTRIBUTE));

            String groupKeyString = componentAttributes.getString(XML_GROUP_KEY_FIELDS_ATTRIBUTE);
            rollup.setGroupKeyFields(!StringUtils.isEmpty(groupKeyString)
                    ? groupKeyString.trim().split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX) : null);
            rollup.setGroupAccumulatorMetadataId(
                    componentAttributes.getString(XML_GROUP_ACCUMULATOR_METADATA_ID_ATTRIBUTE, null));

            rollup.setTransform(componentAttributes.getString(XML_TRANSFORM_ATTRIBUTE, null));
            rollup.setTransformUrl(componentAttributes.getString(XML_TRANSFORM_URL_ATTRIBUTE, null));
            rollup.setTransformUrlCharset(componentAttributes.getString(XML_TRANSFORM_URL_CHARSET_ATTRIBUTE,
                    Defaults.DataParser.DEFAULT_CHARSET_DECODER));
            rollup.setTransformClassName(componentAttributes.getString(XML_TRANSFORM_CLASS_NAME_ATTRIBUTE, null));

            rollup.setInputSorted(componentAttributes.getBoolean(XML_INPUT_SORTED_ATTRIBUTE, false));
        } catch (AttributeNotFoundException exception) {
            throw new XMLConfigurationException("Missing a required attribute!", exception);
        } catch (Exception exception) {
            throw new XMLConfigurationException("Error creating the component!", exception);
        } catch (Throwable tw) {
            throw new XMLConfigurationException(tw);
        }

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

    /** the flag specifying whether the input data records are sorted or not */
    private boolean inputSorted;

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

    public void setInputSorted(boolean inputSorted) {
        this.inputSorted = inputSorted;
    }

    @Override
    public void toXML(Element xmlElement) {
        super.toXML(xmlElement);

        if (groupKeyFields != null) {
            xmlElement.setAttribute(XML_GROUP_KEY_FIELDS_ATTRIBUTE,
                    StringUtils.stringArraytoString(groupKeyFields, Defaults.Component.KEY_FIELDS_DELIMITER));
        }

        if (!StringUtils.isEmpty(groupAccumulatorMetadataId)) {
            xmlElement.setAttribute(XML_GROUP_ACCUMULATOR_METADATA_ID_ATTRIBUTE, groupAccumulatorMetadataId);
        }

        if (!StringUtils.isEmpty(transform)) {
            xmlElement.setAttribute(XML_TRANSFORM_ATTRIBUTE, transform);
        }

        if (!StringUtils.isEmpty(transformUrl)) {
            xmlElement.setAttribute(XML_TRANSFORM_URL_ATTRIBUTE, transformUrl);
        }

        if (!StringUtils.isEmpty(transformUrlCharset)) {
            xmlElement.setAttribute(XML_TRANSFORM_URL_CHARSET_ATTRIBUTE, transformUrlCharset);
        }

        if (!StringUtils.isEmpty(transformClassName)) {
            xmlElement.setAttribute(XML_TRANSFORM_CLASS_NAME_ATTRIBUTE, transformClassName);
        }

        if (inputSorted) {
            xmlElement.setAttribute(XML_INPUT_SORTED_ATTRIBUTE, Boolean.toString(inputSorted));
        }
    }

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 1, Integer.MAX_VALUE);
        checkMetadata(status, getInMetadata(), getOutMetadata());

        if (groupKeyFields == null || groupKeyFields.length == 0) {
            status.add(new ConfigurationProblem("No group key fields specified!",
                    Severity.ERROR, this, Priority.HIGH, "groupKeyFields"));
        } else {
            DataRecordMetadata metadata = getInputPort(INPUT_PORT_NUMBER).getMetadata();

            for (String groupKeyField : groupKeyFields) {
                if (metadata.getField(groupKeyField) == null) {
                    status.add(new ConfigurationProblem("The group key field " + StringUtils.quote(groupKeyField)
                            + " doesn't exist!", Severity.ERROR, this, Priority.HIGH, "groupKeyFields"));
                }
            }
        }

        if (groupAccumulatorMetadataId != null && getGraph().getDataRecordMetadata(groupAccumulatorMetadataId) == null) {
            status.add(new ConfigurationProblem("The group \"accumulator\" meta data ID is not valid!",
                    Severity.ERROR, this, Priority.HIGH, "groupAccumulatorMetadataId"));
        }

        if (StringUtils.isEmpty(transform) && StringUtils.isEmpty(transformUrl) && StringUtils.isEmpty(transformClassName)) {
            status.add(new ConfigurationProblem("No rollup transform specified!", Severity.ERROR, this, Priority.HIGH));
        }

        if (transformUrlCharset != null && !Charset.isSupported(transformUrlCharset)) {
            status.add(new ConfigurationProblem("The transform URL character set is not supported!",
                    Severity.ERROR, this, Priority.NORMAL, "transformUrlCharset"));
        }

        return status;
    }

    @Override
    public synchronized void init() throws ComponentNotReadyException {
        if (isInitialized()) {
            throw new IllegalStateException("The component has already been initialized!");
        }

        super.init();

        groupKey = new RecordKey(groupKeyFields, getInputPort(INPUT_PORT_NUMBER).getMetadata());
        groupKey.init();

        if (transform != null) {
            recordRollup = createTransformFromSourceCode(transform);
        } else if (transformUrl != null) {
            recordRollup = createTransformFromSourceCode(FileUtils.getStringFromURL(
                    getGraph().getProjectURL(), transformUrl, transformUrlCharset));
        } else if (transformClassName != null) {
            recordRollup = createTransformFromClassName(transformClassName);
        }

        recordRollup.init(null, getInputPort(INPUT_PORT_NUMBER).getMetadata(),
                getGraph().getDataRecordMetadata(groupAccumulatorMetadataId),
                getOutMetadata().toArray(new DataRecordMetadata[getOutPorts().size()]));

        outputRecords = new DataRecord[getOutPorts().size()];

        for (int i = 0; i < outputRecords.length; i++) {
            outputRecords[i] = new DataRecord(getOutputPort(i).getMetadata());
            outputRecords[i].init();
        }
    }

    /**
     * Creates a rollup transform using the given source code.
     *
     * @param sourceCode a Java or CTL source code to be used
     *
     * @return an instance of the <code>RecordRollup</code> transform
     *
     * @throws ComponentNotReadyException if an error occurred during the instantiation of the transform
     * or if the type of the transformation could not be determined
     */
    private RecordRollup createTransformFromSourceCode(String sourceCode) throws ComponentNotReadyException {
        if (sourceCode.indexOf(WrapperTL.TL_TRANSFORM_CODE_ID) >= 0
                || Pattern.compile(REGEX_TL_CODE).matcher(sourceCode).find()) {
            return new RecordRollupTL(sourceCode, getGraph());
        }

        if (Pattern.compile(REGEX_JAVA_CLASS).matcher(sourceCode).find()) {
            try {
                return (RecordRollup) new DynamicJavaCode(sourceCode, getClass().getClassLoader()).instantiate();
            } catch (ClassCastException exception) {
                throw new ComponentNotReadyException(
                        "The transformation code does not implement the RecordRollup interface!", exception);
            } catch (RuntimeException exception) {
                throw new ComponentNotReadyException("Cannot compile the transformation code!", exception);
            }
        }

        throw new ComponentNotReadyException("Cannot determine the type of the transformation code!");
    }

    /**
     * Creates a rollup transform using a Java class with the given class name.
     *
     * @param className a class name of a class to be instantiated
     *
     * @return an instance of the <code>RecordRollup</code> transform
     *
     * @throws ComponentNotReadyException if an error occurred during the instantiation of the transform
     */
    private RecordRollup createTransformFromClassName(String className) throws ComponentNotReadyException {
        try {
            return (RecordRollup) Class.forName(transformClassName).newInstance();
        } catch (ClassNotFoundException exception) {
            throw new ComponentNotReadyException("Cannot find the transformation class!", exception);
        } catch (IllegalAccessException exception) {
            throw new ComponentNotReadyException("Cannot access the transformation class!", exception);
        } catch (InstantiationException exception) {
            throw new ComponentNotReadyException("Cannot instantiate the transformation class!", exception);
        } catch (ClassCastException exception) {
            throw new ComponentNotReadyException(
                    "The transformation class does not implement the RecordRollup interface!", exception);
        }
    }

    @Override
    public Result execute() throws Exception {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        if (inputSorted) {
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
     * @throws TransformException if an error occurred during the transformation
     * @throws IOException if an error occurred while reading or writing data records
     * @throws InterruptedException if an error occurred while reading or writing data records
     */
    private void executeInputSorted() throws TransformException, IOException, InterruptedException {
        InputPort inputPort = getInputPort(INPUT_PORT_NUMBER);

        DoubleRecordBuffer inputRecords = new DoubleRecordBuffer(inputPort.getMetadata());
        DataRecord groupAccumulator = null;

        if (groupAccumulatorMetadataId != null) {
            groupAccumulator = new DataRecord(getGraph().getDataRecordMetadata(groupAccumulatorMetadataId));
            groupAccumulator.init();
        }

        if (inputPort.readRecord(inputRecords.getCurrent()) != null) {
            recordRollup.initGroup(inputRecords.getCurrent(), groupAccumulator);

            if (recordRollup.updateGroup(inputRecords.getCurrent(), groupAccumulator)) {
                transform(inputRecords.getCurrent(), groupAccumulator);
            }

            inputRecords.swap();

            while (runIt && inputPort.readRecord(inputRecords.getCurrent()) != null) {
                if (!groupKey.equals(inputRecords.getCurrent(), inputRecords.getPrevious())) {
                    if (recordRollup.finishGroup(inputRecords.getPrevious(), groupAccumulator)) {
                        transform(inputRecords.getPrevious(), groupAccumulator);
                    }

                    groupAccumulator.reset();
                    recordRollup.initGroup(inputRecords.getCurrent(), groupAccumulator);
                }

                if (recordRollup.updateGroup(inputRecords.getCurrent(), groupAccumulator)) {
                    transform(inputRecords.getCurrent(), groupAccumulator);
                }

                inputRecords.swap();
                SynchronizeUtils.cloverYield();
            }

            if (recordRollup.finishGroup(inputRecords.getPrevious(), groupAccumulator)) {
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

        DataRecord inputRecord = new DataRecord(inputPort.getMetadata());
        inputRecord.init();

        Map<HashKey, DataRecord> groupAccumulators = new HashMap<HashKey, DataRecord>();
        HashKey lookupKey = new HashKey(groupKey, inputRecord);

        while (runIt && inputPort.readRecord(inputRecord) != null) {
            DataRecord groupAccumulator = groupAccumulators.get(lookupKey);

            if (groupAccumulator == null && !groupAccumulators.containsKey(lookupKey)) {
                if (groupAccumulatorMetadataId != null) {
                    groupAccumulator = new DataRecord(getGraph().getDataRecordMetadata(groupAccumulatorMetadataId));
                    groupAccumulator.init();
                }

                groupAccumulators.put(new HashKey(groupKey, inputRecord.duplicate()), groupAccumulator);
            }

            recordRollup.initGroup(inputRecord, groupAccumulator);

            if (recordRollup.updateGroup(inputRecord, groupAccumulator)) {
                transform(inputRecord, groupAccumulator);
            }

            SynchronizeUtils.cloverYield();
        }

        for (Map.Entry<HashKey, DataRecord> entry : groupAccumulators.entrySet()) {
            if (recordRollup.finishGroup(entry.getKey().getDataRecord(), entry.getValue())) {
                transform(entry.getKey().getDataRecord(), entry.getValue());
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
        for (DataRecord outputRecord : outputRecords) {
            outputRecord.reset();
        }

        int transformResult = recordRollup.transform(inputRecord, groupAccumulator, outputRecords);

        while (transformResult != RecordRollup.SKIP) {
            if (transformResult == RecordRollup.ALL) {
                for (int i = 0; i < outputRecords.length; i++) {
                    writeRecord(i, outputRecords[i]);
                }
            } else if (transformResult >= 0) {
                writeRecord(transformResult, outputRecords[transformResult]);
            } else {
                throw new TransformException("Transform finished with error " + transformResult + "!");
            }
        }
    }

    @Override
    public synchronized void reset() throws ComponentNotReadyException {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        recordRollup.reset();

        for (DataRecord outputRecord : outputRecords) {
            outputRecord.reset();
        }

        super.reset();
    }

    @Override
    public synchronized void free() {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        groupKey = null;

        recordRollup.free();
        recordRollup = null;

        outputRecords = null;

        super.free();
    }

}

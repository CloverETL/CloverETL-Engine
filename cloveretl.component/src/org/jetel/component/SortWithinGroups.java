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

import org.jetel.data.Defaults;
import org.jetel.data.DoubleRecordBuffer;
import org.jetel.data.ExternalSortDataRecord;
import org.jetel.data.ISortDataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.CopyComponentTokenTracker;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>SortWithinGroups Component</h3>
 *
 * <p>Sorts the incoming data records based on the sort key within the specified group.</p>
 *
 * <h4>Component:</h4>
 * <table border="1">
 * <tr>
 *   <td><b>Name:</b></td>
 *   <td>SortWithinGroups</td>
 * </tr>
 * <tr>
 *   <td><b>Category:</b></td>
 *   <td>transformers</td>
 * </tr>
 * <tr>
 *   <td><b>Description:</b></td>
 *   <td>
 *     Receives data records through connected input port and sorts them according to the
 *     specified sort key within a group specified by the group key. Sorted data records are
 *     sent to all the connected output ports. The sort key (as well as the group key) is
 *     a name or a combination of names of field(s) of incoming data records. Sort order
 *     of each data field can be either Ascending (default) or Descending. Any number
 *     of records can be sorted. If the internal buffer is full, external sorting is performed.
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
 * <tr>
 *   <td><b>Comment:</b></td>
 *   <td>N/A</td>
 * </tr>
 * </table>
 * 
 * <h4>XML attributes:</h4>
 * <table border="1">
 * <tr>
 *   <td><b>id</b></td>
 *   <td>A component identification.</td>
 * </tr>
 * <tr>
 *   <td><b>type</b></td>
 *   <td>SORT_WITHIN_GROUPS</td>
 * </tr>
 * <tr>
 *   <td><b>groupKey</b></td>
 *   <td>Field names separated by ':', ';' or '|'.</td>
 * </tr>
 * <tr>
 *   <td><b>sortKey</b></td>
 *   <td>
 *     Field names followed by "(a)" (ascending order - default) or "(d)" (descending
 *     order) and separated by ':', ';' or '|'.
 *   </td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>bufferCapacity</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>
 *     Defines the maximum number of records that will be sorted in memory. If the number
 *     of records exceeds this value, external sorting is performed.
 *   </td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>numberOfTapes</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>
 *     Denotes how many tapes (temporary files) will be used for external data sorting. This
 *     value must be an even number greater than 2. <i>The default value is 8.</i>
 *   </td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>tempDirectories</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>
 *     A semicolon-delimited list of directories which should be used for creating tape files.
 *     These are used when external sorting is performed. The default value is equal to Java's
 *     <code>java.io.tmpdir</code> system property.
 *   </td>
 * </tr>
 * </table>
 *
 * <h4>Example:</h4>
 * <pre>
 *   &lt;Node id="SORT_CUSTOMER" type="SORT_WITHIN_GROUPS"
 *       groupKey="id" sortKey="name(a);address(a)"/&gt;
 * </pre>
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 24th June 2009
 * @since 26th September 2008
 */
public class SortWithinGroups extends Node {

    /** the type of the component */
    private static final String COMPONENT_TYPE = "SORT_WITHIN_GROUPS";

    /** the XML attribute used to store the group key */
    private static final String XML_ATTRIBUTE_GROUP_KEY = "groupKey";
    /** the XML attribute used to store the sort key */
    private static final String XML_ATTRIBUTE_SORT_KEY = "sortKey";
    /** the XML attribute used to store the buffer capacity */
    private static final String XML_ATTRIBUTE_BUFFER_CAPACITY = "bufferCapacity";
    /** the XML attribute used to store the number of tapes */
    private static final String XML_ATTRIBUTE_NUMBER_OF_TAPES = "numberOfTapes";
    
    /** the ascending sort order */
    private static final char SORT_ASCENDING = 'a';
    /** the descending sort order */
    private static final char SORT_DESCENDING = 'd';

    /** the port index used for data record input */
    private static final int INPUT_PORT_NUMBER = 0;

    /** the default buffer capacity */
    private static final int DEFAULT_BUFFER_CAPACITY = -1;
    /** the default number of tapes */
    private static final int DEFAULT_NUMBER_OF_TAPES = 8;
    
    /**
     * Creates an instance of the <code>SortWithinGroups</code> component from an XML element.
     *
     * @param transformationGraph the transformation graph the component belongs to
     * @param xmlElement the XML element that should be used for construction
     *
     * @return an instance of the <code>SortWithinGroups</code> component
     *
     * @throws XMLConfigurationException when some attribute is missing
     * @throws AttributeNotFoundException 
     */
    public static Node fromXML(TransformationGraph transformationGraph, Element xmlElement)
            throws XMLConfigurationException, AttributeNotFoundException {
        SortWithinGroups sortWithinGroups = null;

        ComponentXMLAttributes componentAttributes = new ComponentXMLAttributes(xmlElement, transformationGraph);

        if (!componentAttributes.getString(XML_TYPE_ATTRIBUTE).equalsIgnoreCase(COMPONENT_TYPE)) {
            throw new XMLConfigurationException("The " + StringUtils.quote(XML_TYPE_ATTRIBUTE)
                    + " attribute contains a value incompatible with this component!");
        }

        String groupKey = componentAttributes.getString(XML_ATTRIBUTE_GROUP_KEY);
        String sortKey = componentAttributes.getString(XML_ATTRIBUTE_SORT_KEY);

        sortWithinGroups = new SortWithinGroups(componentAttributes.getString(XML_ID_ATTRIBUTE),
                groupKey.trim().split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
                sortKey.trim().split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

        if (componentAttributes.exists(XML_NAME_ATTRIBUTE)) {
            sortWithinGroups.setName(componentAttributes.getString(XML_NAME_ATTRIBUTE));
        }

        if (componentAttributes.exists(XML_ATTRIBUTE_BUFFER_CAPACITY)) {
            sortWithinGroups.setBufferCapacity(componentAttributes.getInteger(XML_ATTRIBUTE_BUFFER_CAPACITY));
        }

        if (componentAttributes.exists(XML_ATTRIBUTE_NUMBER_OF_TAPES)) {
            sortWithinGroups.setNumberOfTapes(componentAttributes.getInteger(XML_ATTRIBUTE_NUMBER_OF_TAPES));
        }

        return sortWithinGroups;
    }

    /** the key specifying groups of records */
    private final String[] groupKeyFields;

    /** the key that will be used to sort within the groups */
    private final String[] sortKeyFields;
    /** the sort order used for sorting within the groups */
    private final boolean[] sortKeyOrdering;

    /** the buffer capacity used by the data record sorter */
    private int bufferCapacity = DEFAULT_BUFFER_CAPACITY;
    /** the number of tapes used by the data record sorter */
    private int numberOfTapes = DEFAULT_NUMBER_OF_TAPES;
    
    /** a data record sorter used to sort the record groups */
    private ISortDataRecord dataRecordSorter = null;
    /** a data record buffer used to retrieve data records from the above sorter */
    private CloverBuffer dataRecordBuffer = null;

    /**
     * Constructs a new instance of the <code>SortWithinGroups</code> component.
     *
     * @param id an identification of the component
     * @param groupKeyFields an array of group key fields (field name)
     * @param sortKeyFields an array of sort key fields (field name + ordering)
     */
    public SortWithinGroups(String id, String[] groupKeyFields, String[] sortKeyFields) {
        super(id);

        if (groupKeyFields != null && groupKeyFields.length > 0) {
            this.groupKeyFields = groupKeyFields;
        } else {
            this.groupKeyFields = null;
        }

        if (sortKeyFields != null && sortKeyFields.length > 0) {
            this.sortKeyFields = sortKeyFields;
            this.sortKeyOrdering = new boolean[sortKeyFields.length];

            for (int i = 0; i < sortKeyFields.length; i++) {
                String[] sortKeyFieldParts = sortKeyFields[i].split("\\s*\\(\\s*", 2);

                this.sortKeyFields[i] = sortKeyFieldParts[0];
                this.sortKeyOrdering[i] =
                    (sortKeyFieldParts.length == 2) ? sortKeyFieldParts[1].matches("^[Aa].*$") : true;
            }
        } else {
            this.sortKeyFields = null;
            this.sortKeyOrdering = null;
        }
    }

    @Override
    public String getType() {
        return COMPONENT_TYPE;
    }

    public void setBufferCapacity(int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
    }

    public int getBufferCapacity() {
        return bufferCapacity;
    }

    public void setNumberOfTapes(int numberOfTapes) {
        this.numberOfTapes = numberOfTapes;
    }

    public int getNumberOfTapes() {
        return numberOfTapes;
    }

    private String formatSortKey(String[] sortKeyFields, boolean[] sortKeyOrdering) {
        StringBuilder stringBuilder = new StringBuilder();

        for (int i = 0; i < sortKeyFields.length; i++) {
            stringBuilder.append(sortKeyFields[i]);
            stringBuilder.append('(');
            stringBuilder.append(sortKeyOrdering[i] ? SORT_ASCENDING : SORT_DESCENDING);
            stringBuilder.append(')');

            if (i < sortKeyFields.length - 1) {
                stringBuilder.append(Defaults.Component.KEY_FIELDS_DELIMITER);
            }
        }

        return stringBuilder.toString();
    }

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 1, Integer.MAX_VALUE);
        checkMetadata(status, getInMetadata(), getOutMetadata());

        DataRecordMetadata metadata = getInputPort(INPUT_PORT_NUMBER).getMetadata();

        if (groupKeyFields == null) {
            status.add(new ConfigurationProblem("The group key is empty!", Severity.ERROR, this, Priority.HIGH));
        } else {
            for (String groupKeyField : groupKeyFields) {
                if (metadata.getField(groupKeyField) == null) {
                    status.add(new ConfigurationProblem("The group key field " + StringUtils.quote(groupKeyField)
                            + " doesn't exist!", Severity.ERROR, this, Priority.HIGH));
                }
            }
        }

        if (sortKeyFields == null) {
            status.add(new ConfigurationProblem("The sort key is empty!", Severity.ERROR, this, Priority.HIGH));
        } else {
            for (String sortKeyField : sortKeyFields) {
                if (metadata.getField(sortKeyField) == null) {
                    status.add(new ConfigurationProblem("The sort key field " + StringUtils.quote(sortKeyField)
                            + " doesn't exist!", Severity.ERROR, this, Priority.HIGH));
                }
            }
        }

        if (numberOfTapes <= 0) {
            status.add(new ConfigurationProblem("The number of tapes is less than 1!",
                    Severity.ERROR, this, Priority.NORMAL));
        }

        return status;
    }

    @Override
    public synchronized void init() throws ComponentNotReadyException {
        if (isInitialized()) {
            throw new IllegalStateException("The component has already been initialized!");
        }

        super.init();

        try {
            dataRecordSorter = new ExternalSortDataRecord(getInputPort(INPUT_PORT_NUMBER).getMetadata(),
                    sortKeyFields, sortKeyOrdering, bufferCapacity, numberOfTapes);
        } catch (Exception exception) {
            throw new ComponentNotReadyException("Error creating a data record sorter!", exception);
        }

        dataRecordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
    }
    
    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    	    dataRecordSorter.reset();
    	}
    }    

    
    @Override
    public Result execute() throws Exception {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        InputPort inputPort = getInputPort(INPUT_PORT_NUMBER);

        RecordKey groupKey = new RecordKey(groupKeyFields, inputPort.getMetadata());
        groupKey.init();

        DoubleRecordBuffer inputRecords = new DoubleRecordBuffer(inputPort.getMetadata());

        if (inputPort.readRecord(inputRecords.getCurrent()) != null) {
            dataRecordSorter.put(inputRecords.getCurrent());
            inputRecords.swap();

            while (runIt && inputPort.readRecord(inputRecords.getCurrent()) != null) {
                if (!groupKey.equals(inputRecords.getCurrent(), inputRecords.getPrevious())) {
                    dataRecordSorter.sort();

                    while (runIt && dataRecordSorter.get(dataRecordBuffer)) {
                        writeRecordBroadcastDirect(dataRecordBuffer);
                        dataRecordBuffer.clear();
                    }

                    dataRecordSorter.reset();
                }

                dataRecordSorter.put(inputRecords.getCurrent());
                inputRecords.swap();

                SynchronizeUtils.cloverYield();
            }

            dataRecordSorter.sort();

            while (runIt && dataRecordSorter.get(dataRecordBuffer)) {
                writeRecordBroadcastDirect(dataRecordBuffer);
                dataRecordBuffer.clear();
            }
        }

        broadcastEOF();

        return (runIt ? Result.FINISHED_OK : Result.ABORTED);
    }

    @Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		dataRecordSorter.postExecute();
	}

    @Override
    public synchronized void free() {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

        super.free();

        if (dataRecordSorter != null) {
            try {
                dataRecordSorter.free();
            } catch (InterruptedException exception) {
                // OK, don't do anything
            }
        }
    }

    @Override
    protected ComponentTokenTracker createComponentTokenTracker() {
    	return new CopyComponentTokenTracker(this);
    }

}

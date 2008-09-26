/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2008  David Pavlis <david.pavlis@javlin.cz>
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

import java.nio.ByteBuffer;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.ExternalSortDataRecord;
import org.jetel.data.ISortDataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.SynchronizeUtils;
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
 *     specified sort key within group specified by the group key. Sorted data records are
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
 *   <td><b>Comment:></b></td>
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
 *     of records exceed this value, external sorting is performed.
 *   </td>
 * </tr>
 * <tr>
 *   <td>
 *     <b>numberOfTapes</b><br>
 *     <i>optional</i>
 *   </td>
 *   <td>
 *     Denotes how many tapes (temporary files) will be used for external data sorting. This
 *     value must be a even number greater than 2. <i>The default value is 8.</i>
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
 *       groupKey="id" sortKey="name(a):address(a)"/&gt;
 * </pre>
 *
 * @author mjanik
 * @since 26th September 2008
 */
public class SortWithinGroups extends Node {

    /** the type of the component */
    private static final String COMPONENT_TYPE = "SORT_WITHIN_GROUPS";

    /** the XML attribute used to store the group key */
    private static final String XML_GROUP_KEY_ATTRIBUTE = "groupKey";
    /** the XML attribute used to store the sort key */
    private static final String XML_SORT_KEY_ATTRIBUTE = "sortKey";
    /** the XML attribute used to store the buffer capacity */
    private static final String XML_BUFFER_CAPACITY_ATTRIBUTE = "bufferCapacity";
    /** the XML attribute used to store the number of tapes */
    private static final String XML_NUMBER_OF_TAPES_ATTRIBUTE = "numberOfTapes";
    /** the XML attribute used to store the temporary directories */
    private static final String XML_TEMP_DIRECTORIES_ATTRIBUTE = "tempDirectories";

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
    /** the default temporary directories */
    private static final String[] DEFAULT_TEMP_DIRECTORIES = null;

    /**
     * Creates an instance of the SortWithinGroups component from a XML element.
     *
     * @param transformationGraph the transformation graph the component belongs to
     * @param xmlElement the XML element that should be used for construction
     *
     * @return an instance of the SortWithinGroups component
     *
     * @throws XMLConfigurationException when some attribute is missing
     */
    public static Node fromXML(TransformationGraph transformationGraph, Element xmlElement)
            throws XMLConfigurationException {
        SortWithinGroups sortWithinGroups = null;

        ComponentXMLAttributes componentAttributes =
                new ComponentXMLAttributes(xmlElement, transformationGraph);

        try {
            String groupKey = componentAttributes.getString(XML_GROUP_KEY_ATTRIBUTE);
            String sortKey = componentAttributes.getString(XML_SORT_KEY_ATTRIBUTE);

            sortWithinGroups = new SortWithinGroups(
                    componentAttributes.getString(XML_ID_ATTRIBUTE),
                    groupKey.trim().split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
                    sortKey.trim().split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

            if (componentAttributes.exists(XML_BUFFER_CAPACITY_ATTRIBUTE)) {
                sortWithinGroups.setBufferCapacity(
                        componentAttributes.getInteger(XML_BUFFER_CAPACITY_ATTRIBUTE));
            }

            if (componentAttributes.exists(XML_NUMBER_OF_TAPES_ATTRIBUTE)) {
                sortWithinGroups.setNumberOfTapes(
                        componentAttributes.getInteger(XML_NUMBER_OF_TAPES_ATTRIBUTE));
            }

            if (componentAttributes.exists(XML_TEMP_DIRECTORIES_ATTRIBUTE)) {
                sortWithinGroups.setTempDirectories(
                        componentAttributes.getString(XML_TEMP_DIRECTORIES_ATTRIBUTE)
                            .split(Defaults.DEFAULT_PATH_SEPARATOR_REGEX));
            }
        } catch (Exception exception) {
            throw new XMLConfigurationException("Error loading the component!", exception);
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
    /** temporary directories used by the data record sorter */
    private String[] tempDirectories = DEFAULT_TEMP_DIRECTORIES;

    /** a data record sorter used to sort the record groups */
    private ISortDataRecord dataRecordSorter = null;
    /** a data record buffer used to retrieve data records from the above sorter */
    private ByteBuffer dataRecordBuffer = null;

    /**
     * Constructs an instance of the SortWithinGroups component.
     *
     * @param id an identification of the component
     * @param groupKeyParts an array of group key parts (field name + order)
     * @param sortKeyParts an array of sort key parts (field name + order)
     */
    public SortWithinGroups(String id, String[] groupKeyParts, String[] sortKeyParts) {
        super(id);

        this.groupKeyFields = new String[groupKeyParts.length];

        for (int i = 0; i < groupKeyParts.length; i++) {
            this.groupKeyFields[i] = groupKeyParts[i].replaceFirst("\\s*\\(.*$", "");
        }

        this.sortKeyFields = new String[sortKeyParts.length];
        this.sortKeyOrdering = new boolean[sortKeyParts.length];

        for (int i = 0; i < sortKeyParts.length; i++) {
            String[] sortKeyPartParts = sortKeyParts[i].split("\\s*\\(\\s*");

            this.sortKeyFields[i] = sortKeyPartParts[0];
            this.sortKeyOrdering[i] = sortKeyPartParts[1].matches("^[Aa].*$");
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

    public void setTempDirectories(String[] tempDirectories) {
        this.tempDirectories = tempDirectories;
    }

    public String[] getTempDirectories() {
        return tempDirectories;
    }

    @Override
    public void toXML(Element xmlElement) {
        super.toXML(xmlElement);

        xmlElement.setAttribute(XML_GROUP_KEY_ATTRIBUTE,
                StringUtils.stringArraytoString(groupKeyFields,
                    Defaults.Component.KEY_FIELDS_DELIMITER));
        xmlElement.setAttribute(XML_SORT_KEY_ATTRIBUTE,
                formatSortKey(sortKeyFields, sortKeyOrdering));

        if (bufferCapacity != DEFAULT_BUFFER_CAPACITY) {
            xmlElement.setAttribute(XML_BUFFER_CAPACITY_ATTRIBUTE,
                    Integer.toString(bufferCapacity));
        }

        if (numberOfTapes != DEFAULT_NUMBER_OF_TAPES) {
            xmlElement.setAttribute(XML_NUMBER_OF_TAPES_ATTRIBUTE,
                    Integer.toString(numberOfTapes));
        }

        if (tempDirectories != DEFAULT_TEMP_DIRECTORIES) {
            xmlElement.setAttribute(XML_TEMP_DIRECTORIES_ATTRIBUTE,
                    StringUtils.stringArraytoString(tempDirectories,
                        Defaults.DEFAULT_PATH_SEPARATOR_REGEX));
        }
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

        return status;
    }

    @Override
    public synchronized void init() throws ComponentNotReadyException {
        if (isInitialized()) {
            throw new IllegalStateException("The component has already been initialized!");
        }

        super.init();

        try {
            dataRecordSorter = new ExternalSortDataRecord(
                    getInputPort(INPUT_PORT_NUMBER).getMetadata(), sortKeyFields,
                    sortKeyOrdering, bufferCapacity, numberOfTapes, tempDirectories);
        } catch (Exception exception) {
            throw new ComponentNotReadyException("Error creating a data record sorter!",
                    exception);
        }

        dataRecordBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);

        if (dataRecordBuffer == null) {
            throw new ComponentNotReadyException("Data record buffer allocation failed! "
                    + "Required size: " + Defaults.Record.MAX_RECORD_SIZE);
        }
    }

    @Override
    public Result execute() throws Exception {
        InputPort inputPort = getInputPort(INPUT_PORT_NUMBER);

        RecordKey groupKey = new RecordKey(groupKeyFields, inputPort.getMetadata());
        groupKey.init();

        DataRecord groupDataRecord = null;

        DataRecord dataRecord = new DataRecord(inputPort.getMetadata());
        dataRecord.init();

        while (runIt && inputPort.readRecord(dataRecord) != null) {
            if (groupDataRecord == null) {
                groupDataRecord = dataRecord.duplicate();
            } else if (!groupKey.equals(dataRecord, groupDataRecord)) {
                dataRecordSorter.sort();

                while (runIt && dataRecordSorter.get(dataRecordBuffer)) {
                    writeRecordBroadcastDirect(dataRecordBuffer);
                    dataRecordBuffer.clear();
                }

                dataRecordSorter.reset();

                groupDataRecord = dataRecord.duplicate();
            }

            dataRecordSorter.put(dataRecord);

            SynchronizeUtils.cloverYield();
        }

        dataRecordSorter.sort();

        while (dataRecordSorter.get(dataRecordBuffer)) {
            writeRecordBroadcastDirect(dataRecordBuffer);
            dataRecordBuffer.clear();
        }

        broadcastEOF();

        return (runIt ? Result.FINISHED_OK : Result.ABORTED);
    }

    @Override
    public synchronized void reset() throws ComponentNotReadyException {
        if (!isInitialized()) {
            throw new IllegalStateException("The component has NOT been initialized!");
        }

        super.reset();

        dataRecordSorter.reset();
    }

    @Override
    public synchronized void free() {
        if (!isInitialized()) {
            throw new IllegalStateException("The component has NOT been initialized!");
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
}

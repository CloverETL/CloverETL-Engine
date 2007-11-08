/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.component;

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Dedup Component</h3>
 *
 * <!-- Removes duplicates (based on specified key) from data flow of sorted records-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Dedup</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Dedup (remove duplicate records) from sorted incoming records based on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  It keeps either First or Last record from the group based on the parameter <emp>{keep}</emp> specified.
 *  All duplicated records are rejected to the second optional port.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- result of deduplication</td></tr>
 * <td>[0]- all rejected records</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DEDUP"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>dedupKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe} or can be empty, then all records belong to one group</td>
 *  <tr><td><b>keep</b></td><td>one of "First|Last|Unique" {the fist letter is sufficient, if not defined, then First}</td></tr>
 *  <tr><td><b>equalNULL</b><br><i>optional</i></td><td>specifies whether two fields containing NULL values are considered equal. Default is TRUE.</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="DISTINCT" type="DEDUP" dedupKey="Name" keep="First"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 */
public class Dedup extends Node {

	private static final String XML_KEEP_ATTRIBUTE = "keep";
	private static final String XML_DEDUPKEY_ATTRIBUTE = "dedupKey";
	private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DEDUP";

	private final static int READ_FROM_PORT = 0;

    private final static int WRITE_TO_PORT = 0;
    private final static int REJECTED_PORT = 1;

	private final static int KEEP_FIRST = 1;
	private final static int KEEP_LAST = -1;
	private final static int KEEP_UNIQUE = 0;
	

	private int keep;
	private String[] dedupKeys;
	private RecordKey recordKey;
	private boolean equalNULLs = true;
	private boolean hasRejectedPort;

    //runtime variables
    int current;
    int previous;
    boolean isFirst;
    InputPort inPort;
    DataRecord[] records;

    
	/**
	 *Constructor for the Dedup object
	 *
	 * @param  id         unique id of the component
	 * @param  dedupKeys  definitio of key fields used to compare records
	 * @param  keep  (1 - keep first; 0 - keep unique; -1 - keep last)
	 */
	public Dedup(String id, String[] dedupKeys, int keep) {
		super(id);
		this.keep = keep;
		this.dedupKeys = dedupKeys;
	}


	/**
	 *  Gets the change attribute of the Dedup object
	 *
	 * @param  a  Description of the Parameter
	 * @param  b  Description of the Parameter
	 * @return    The change value
	 */
	private final boolean isChange(DataRecord a, DataRecord b) {
        if(recordKey != null) {
            return (recordKey.compare(a, b) != 0);
		} else {
            return false;
        }
	}

    
	@Override
	public Result execute() throws Exception {
        
        isFirst = true; // special treatment for 1st record
        inPort = getInputPort(READ_FROM_PORT);
        records = new DataRecord[2]; 
        records[0] = new DataRecord(inPort.getMetadata());
        records[0].init();
        records[1] = new DataRecord(inPort.getMetadata());
        records[1].init();
        current = 1;
        previous = 0;

        switch(keep) {
        case KEEP_FIRST:
            executeFirst();
            break;
        case KEEP_LAST:
            executeLast();
            break;
        case KEEP_UNIQUE:
            executeUnique();
            break;
        }
		
        broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

    /**
     * Execution a de-duplication with first function.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
	private void executeFirst() throws IOException, InterruptedException {
        while (records[current] != null && runIt) {
            records[current] = inPort.readRecord(records[current]);
            if (records[current] != null) {
                if (isFirst) {
                    writeRecord(WRITE_TO_PORT, records[current]);
                    isFirst = false;
                } else {
                    if (isChange(records[current], records[previous])) {
                        writeRecord(WRITE_TO_PORT, records[current]);
                    } else {
                        writeRejectedRecord(records[current]);
                    }
                }
                // swap indexes
                current = current ^ 1;
                previous = previous ^ 1;
            }
        }
    }

    /**
     * Execution a de-duplication with last function.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void executeLast() throws IOException, InterruptedException {
        while (records[current] != null && runIt) {
            records[current] = inPort.readRecord(records[current]);
            if (records[current] != null) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    if (isChange(records[current], records[previous])) {
                        writeRecord(WRITE_TO_PORT, records[previous]);
                    } else {
                        writeRejectedRecord(records[previous]);
                    }
                }

                // swap indexes
                current = current ^ 1;
                previous = previous ^ 1;
            } else {
                if (!isFirst) {
                    writeRecord(WRITE_TO_PORT, records[previous]);
                }
            }
        }
    }

    /**
     * Execution a de-duplication with unique function.
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    public void executeUnique() throws IOException, InterruptedException {
        int groupItems = 0;

        while (records[current] != null && runIt) {
            records[current] = inPort.readRecord(records[current]);
            if (records[current] != null) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    if (isChange(records[current], records[previous])) {
                        if (groupItems == 1) {
                            writeRecord(WRITE_TO_PORT, records[previous]);
                        } else {
                            writeRejectedRecord(records[previous]);
                        }
                        groupItems = 0;
                    } else {
                        writeRejectedRecord(records[previous]);
                    }
                }
                groupItems++;
                // swap indexes
                current = current ^ 1;
                previous = previous ^ 1;
            } else {
                if (!isFirst) {
                    if(groupItems == 1) {
                        writeRecord(WRITE_TO_PORT, records[previous]);
                    } else {
                        writeRejectedRecord(records[previous]);
                    }
                }
            }
        }
    }

    
    /**
     * Tries to write given record to the rejected port, if is connected.
     * @param record
     * @throws InterruptedException 
     * @throws IOException 
     */
    private void writeRejectedRecord(DataRecord record) throws IOException, InterruptedException {
        if(hasRejectedPort) {
            writeRecord(REJECTED_PORT, record);
        }
    }
    
	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException
	 *                Description of the Exception
	 * @since April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		super.init();

        if(dedupKeys != null) {
            recordKey = new RecordKey(dedupKeys, getInputPort(READ_FROM_PORT).getMetadata());
            recordKey.init();
            // for DEDUP component, specify whether two fields with NULL value indicator set
            // are considered equal
            recordKey.setEqualNULLs(equalNULLs);
        }
        
        hasRejectedPort = (getOutPorts().size() == 2);
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	@Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		// dedupKeys attribute
		if (dedupKeys != null) {
			String keys = this.dedupKeys[0];
			for (int i=1; i<this.dedupKeys.length; i++) {
				keys += Defaults.Component.KEY_FIELDS_DELIMITER + this.dedupKeys[i];
			}
			xmlElement.setAttribute(XML_DEDUPKEY_ATTRIBUTE,keys);
		}
		
		// keep attribute
		switch(this.keep){
			case KEEP_FIRST: xmlElement.setAttribute(XML_KEEP_ATTRIBUTE, "First");
				break;
			case KEEP_LAST: xmlElement.setAttribute(XML_KEEP_ATTRIBUTE, "Last");
				break;
			case KEEP_UNIQUE: xmlElement.setAttribute(XML_KEEP_ATTRIBUTE, "Unique");
				break;
		}
		
		// equal NULL attribute
		xmlElement.setAttribute(XML_EQUAL_NULL_ATTRIBUTE, String.valueOf(equalNULLs));
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
     public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		Dedup dedup;
		try {
            String dedupKey = xattribs.getString(XML_DEDUPKEY_ATTRIBUTE, null);
            
			dedup=new Dedup(xattribs.getString(XML_ID_ATTRIBUTE),
					dedupKey != null ? dedupKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX) : null,
					xattribs.getString(XML_KEEP_ATTRIBUTE).matches("^[Ff].*") ? KEEP_FIRST :
					    xattribs.getString(XML_KEEP_ATTRIBUTE).matches("^[Ll].*") ? KEEP_LAST : KEEP_UNIQUE);
			if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
			    dedup.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
			}
			
		} catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
		return dedup;
	}


	/**  Description of the Method */
     @Override
     public ConfigurationStatus checkConfig(ConfigurationStatus status) {
         super.checkConfig(status);
         
         checkInputPorts(status, 1, 1);
         checkOutputPorts(status, 1, 2);
         checkMetadata(status, getInputPort(READ_FROM_PORT).getMetadata(), getOutMetadata());

         try {
             init();
             free();
         } catch (ComponentNotReadyException e) {
             ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
             if(!StringUtils.isEmpty(e.getAttributeName())) {
                 problem.setAttributeName(e.getAttributeName());
             }
             status.add(problem);
         }
         
         return status;
     }
	
	public String getType(){
		return COMPONENT_TYPE;
	}
	
	public void setEqualNULLs(boolean equal){
	    this.equalNULLs=equal;
	}
}


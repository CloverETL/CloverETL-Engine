
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.Node.Result;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Dec 11, 2006
 *
 */
public class LookupTableReaderWriter extends Node {

	private static final String XML_LOOKUP_TABLE_ATTRIBUTE = "lookupTable";

	public final static String COMPONENT_TYPE = "LOOKUP_TABLE";

	private String lookupTableName;
	
	private final static int READ_FROM_PORT = 0;
	
	private boolean readFromTable = false;
	private LookupTable lookupTable;

	/**
	 * @param id
	 * @param graph
	 * @param lookupTableName
	 */
	public LookupTableReaderWriter(String id, String lookupTableName) {
		super(id);
		this.lookupTableName = lookupTableName;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}


	@Override
	public void init() throws ComponentNotReadyException {
		if (lookupTable == null) {
        	throw new ComponentNotReadyException("Lookup table \"" + lookupTableName + 
			"\" not found.");
		}
		if (!lookupTable.isInited()) {
			lookupTable.init();
		}		
	}
	
	@Override
	public Result execute() throws Exception {
		if (readFromTable) {
			for (DataRecord record : lookupTable) {
				if (!runIt) break;
				writeRecordBroadcast(record);
			}
		} else {
			InputPort inPort = getInputPort(READ_FROM_PORT);
			DataRecord inRecord = new DataRecord(inPort.getMetadata());
			inRecord.init();
			while ((inRecord = inPort.readRecord(inRecord)) != null && runIt) {
				lookupTable.put(null, inRecord);
			}
		}
		return runIt ? Node.Result.OK : Node.Result.ABORTED;
	}
	
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		try{
			return new LookupTableReaderWriter(xattribs.getString(XML_ID_ATTRIBUTE), 
					xattribs.getString(XML_LOOKUP_TABLE_ATTRIBUTE));
		} catch (Exception ex) {
            throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
 	}
    
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_LOOKUP_TABLE_ATTRIBUTE,this.lookupTable.getId());
	}
    
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
 
		checkInputPorts(status, 0, 1);
        checkOutputPorts(status, 0, Integer.MAX_VALUE);

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
}


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
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
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
	
	private final static int WRITE_TO_PORT = 0;
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

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#run()
	 */
	@Override
	public void run() {
		if (readFromTable) {
			for (DataRecord record : lookupTable) {
				if (!runIt) break;
				try {
//					writeRecordBroadcast(record);
					writeRecord(WRITE_TO_PORT, record);
				} catch (IOException ex) {
					resultMsg = ex.getMessage();
					resultCode = Node.RESULT_ERROR;
					closeAllOutputPorts();
					return;
				} catch (InterruptedException ex) {
					resultMsg = ex.getMessage();
					resultCode = Node.RESULT_ERROR;
					closeAllOutputPorts();
					return;
				}
			}
			lookupTable.free();
			closeAllOutputPorts();
		}else{
			InputPort inPort = getInputPort(READ_FROM_PORT);
			DataRecord inRecord = new DataRecord(inPort.getMetadata());
			inRecord.init();
			try {
				while ((inRecord = inPort.readRecord(inRecord)) != null && runIt) {
					lookupTable.put(null, inRecord);
				}
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (InterruptedException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}
		}
		SynchronizeUtils.cloverYield();
		if (runIt) {
            resultMsg = "OK";
        } else {
            resultMsg = "STOPPED";
        }
		resultCode = Node.RESULT_OK;
		broadcastEOF();
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (outPorts.size() > 0) {
			readFromTable = true;
		}		
		lookupTable = getGraph().getLookupTable(lookupTableName);
		if (lookupTable == null) {
        	throw new ComponentNotReadyException("Lookup table \"" + lookupTableName + 
			"\" not found.");
		}
		lookupTable.init();
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
}


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
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 * @author avackova <agata.vackova@javlinconsulting.cz> ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 12, 2006
 *
 */
public class CloverDataWriter extends Node {

	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";

	public final static String COMPONENT_TYPE = "CLOVER_WRITER";
	private final static int READ_FROM_PORT = 0;
	
	private String fileURL;
	private CloverDataFormatter formatter;
	
	/**
	 * @param id
	 */
	public CloverDataWriter(String id, String fileURL) {
		super(id);
		this.fileURL = fileURL;
		formatter = new CloverDataFormatter();
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
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();
		while (record != null && runIt) {
			try {
				record = inPort.readRecord(record);
				if (record != null) {
					formatter.write(record);
				}
			}
			catch (IOException ex) {
				resultMsg=ex.getMessage();
				resultCode=Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			}
			catch (Exception ex) {
				resultMsg=ex.getClass().getName()+" : "+ ex.getMessage();
				resultCode=Node.RESULT_FATAL_ERROR;
				return;
			}
			SynchronizeUtils.cloverYield();
		}
		formatter.close();
		if (runIt) resultMsg="OK"; else resultMsg="STOPPED";
		resultCode=Node.RESULT_OK;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig()
	 */
	@Override
	public boolean checkConfig() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port
		if (inPorts.size() != 1) {
			throw new ComponentNotReadyException("One input port has to be defined!");
		}
		// based on file mask, create/open output file
		formatter.open(fileURL, getInputPort(READ_FROM_PORT).getMetadata());
	}
	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element nodeXML) {
		ComponentXMLAttributes xattribs=new ComponentXMLAttributes(nodeXML, graph);
		CloverDataWriter aDataWriter = null;
		
		try{
			aDataWriter = new CloverDataWriter(xattribs.getString(Node.XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILEURL_ATTRIBUTE));
		}catch(Exception ex){
			System.err.println(COMPONENT_TYPE + ":" + xattribs.getString(Node.XML_ID_ATTRIBUTE,"unknown ID") + ":" + ex.getMessage());
			return null;
		}
		
		return aDataWriter;
	}

}

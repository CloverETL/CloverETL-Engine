/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.component;

import java.io.*;
import org.jetel.graph.*;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.ComponentXMLAttributes;

/**
 *  <h3>Reformat Component</h3>
 *
 * <!-- Changes / reformats the data between pair of INPUT/OUTPUT ports
 *  This component is only a wrapper around transformation class implementing
 *  org.jetel.component.RecordTransform interface. The method transform
 *  is called for every record passing through this component -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Reformat</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Changes / reformats the data between pair of INPUT/OUTPUT ports.<br>
 *  This component is only a wrapper around transformation class implementing
 *  <i>org.jetel.component.RecordTransform</i> interface. The method <i>transform</i>
 *  is called for every record passing through this component.<br></td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"REFORMAT"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming data</td>
 *  </tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="REF" type="REFORMAT" transformClass="org.jetel.test.reformatOrders"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 */
public class Reformat extends Node {

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "REFORMAT";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;

	private String transformClassName;

	private RecordTransform transformation = null;


	/**
	 *Constructor for the Reformat object
	 *
	 * @param  id              Description of the Parameter
	 * @param  transformClass  Description of the Parameter
	 */
	public Reformat(String id, String transformClass) {
		super(id);
		this.transformClassName = transformClass;
	}


	/**
	 *Constructor for the Reformat object
	 *
	 * @param  id              Description of the Parameter
	 * @param  transformClass  Description of the Parameter
	 */
	public Reformat(String id, RecordTransform transformClass) {
		super(id);
		this.transformation = transformClass;
	}


	/**
	 *  Main processing method for the SimpleCopy object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		DataRecord outRecord = new DataRecord(outPort.getMetadata());
		inRecord.init();
		outRecord.init();

		while (inRecord != null && runIt) {
			try {
				inRecord = readRecord(READ_FROM_PORT, inRecord);
				if (inRecord != null) {
					if (!transformation.transform(inRecord, outRecord)) {
						resultMsg = transformation.getMessage();
						break;
					}
					writeRecord(WRITE_TO_PORT, outRecord);
				}
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_FATAL_ERROR;
				//closeAllOutputPorts();
				return;
			}
		}
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		Class tClass;
		// test that we have at least one input port and one output
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}

		if (transformation == null) {
			// try to load in transformation class & instantiate
			try {
				tClass = Class.forName(transformClassName);
			} catch (ClassNotFoundException ex) {
				throw new ComponentNotReadyException("Can't find specified transformation class: " + transformClassName);
			} catch (Exception ex) {
				throw new ComponentNotReadyException(ex.getMessage());
			}
			try {
				transformation = (RecordTransform) tClass.newInstance();
			} catch (Exception ex) {
				throw new ComponentNotReadyException(ex.getMessage());
			}

		}
		// init transformation
		if (!transformation.init(getInputPort(READ_FROM_PORT).getMetadata(), getOutputPort(WRITE_TO_PORT).getMetadata())) {
			throw new ComponentNotReadyException("Error when initializing reformat function !");
		}
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     May 21, 2002
	 */
	public org.w3c.dom.Node toXML() {
		// TODO
		return null;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);

		try {
			return new Reformat(xattribs.getString("id"),
					xattribs.getString("transformClass"));
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}
	}


	/**  Description of the Method */
	public boolean checkConfig() {
		return true;
	}

}


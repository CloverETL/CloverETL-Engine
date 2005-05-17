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

import java.io.*;
import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.*;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;

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
 *  <tr><td><i>..optional attribute..</i></td><td>any additional attribute is passed to transformation
 * class in Properties object - as a key->value pair. There is no limit to how many optional
 * attributes can be used.</td>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="REF" type="REFORMAT" transformClass="org.jetel.test.reformatOrders" param1="123" param2="XYZ"/&gt;</pre>
 *  <br>
 *  Example with transformation code embedded into graph:<br>
 *  <pre>
 * &lt;Node id="REF" type="REFORMAT"&gt;
 * import org.jetel.component.DataRecordTransform;
 * import org.jetel.metadata.DataRecordMetadata;
 * import org.jetel.data.*;
 *
 * public class reformatOrders extends DataRecordTransform{
 *   DataRecord source,target;
 *   int counter;
 *   String message;
 *	
 *	public boolean transform(DataRecord[] _source, DataRecord[] _target){
 *	  source=_source[0]; target=_target[0];
 *	 	StringBuffer strBuf=new StringBuffer(80);
 *		try{
 *			// mapping among source + target fields
 *			// some fields get assigned directly from source fields, some
 *			// are assigned from internall variables
 *			SetVal.setInt(target,&quot;OrderKey&quot;,counter);
 *			SetVal.setInt(target,&quot;OrderID&quot;,GetVal.getInt(source,&quot;OrderID&quot;));
 *			SetVal.setString(target,&quot;CustomerID&quot;,GetVal.getString(source,&quot;CustomerID&quot;));
 *			SetVal.setValue(target,&quot;OrderDate&quot;,GetVal.getDate(source,&quot;OrderDate&quot;));
 *			SetVal.setString(target,&quot;ShippedDate&quot;,&quot;02.02.1999&quot;);
 *			SetVal.setInt(target,&quot;ShipVia&quot;,GetVal.getInt(source,&quot;ShipVia&quot;));
 *			SetVal.setString(target,&quot;ShipTo&quot;,parameters.getProperty("param2","unknown");
 *		}catch(Exception ex){
 *			message=ex.getMessage()+&quot; -&gt;occured with record :&quot;+counter;
 *			throw new RuntimeException(message);
 *		}
 *		counter++;
 *			return true;
 *	}
 * }
 * &lt;/Node&gt;
 * </pre>
 * <hr>
 * <i><b>Note:</b> DataRecord and in turn its individual fields retain their last assigned value till
 * explicitly changed ba calling <code>setValue()</code>, <code>fromString()</code> or other method.<br>
 * Values are not reset to defaults or NULLs between individual calls to <code>transform()</code> method. It is
 * programmer's responsibility to secure assignment of proper values each time the <code>transform()</code> method
 * is called.</i>
 * <hr>
 * <br> 
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

	private String transformClassName = null;
	private DynamicJavaCode dynamicTransformCode = null;
	private RecordTransform transformation = null;

	private Properties transformationParameters=null;

	/**
	 *Constructor for the Reformat object
	 *
	 * @param  id              unique identification of component
	 * @param  transformClass  Name of transformation CLASS (e.g. org.jetel.test.DemoTrans)
	 */
	public Reformat(String id, String transformClass) {
		super(id);
		this.transformClassName = transformClass;
	}


	/**
	 *Constructor for the Reformat object
	 *
	 * @param  id              unique identification of component
	 * @param  transformClass  Object of class implementing RecordTransform interface
	 */
	public Reformat(String id, RecordTransform transformClass) {
		super(id);
		this.transformation = transformClass;
	}


	/**
	 *Constructor for the Reformat object
	 *
	 * @param  id           unique identification of component
	 * @param  dynamicCode  DynamicJavaCode object
	 */
	public Reformat(String id, DynamicJavaCode dynamicCode) {
		super(id);
		this.dynamicTransformCode = dynamicCode;
	}


	/**
	 *  Main processing method for the reformat object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		OutputPort outPort = getOutputPort(WRITE_TO_PORT);
		DataRecord inRecord[] = {new DataRecord(inPort.getMetadata())};
		DataRecord outRecord[] = { new DataRecord(outPort.getMetadata())};
		inRecord[0].init();
		outRecord[0].init();

		while (inRecord[0] != null && runIt) {
			try {
				inRecord[0] = readRecord(READ_FROM_PORT, inRecord[0]);
				if (inRecord[0] != null) {
					if (transformation.transform(inRecord, outRecord)) {
						writeRecord(WRITE_TO_PORT, outRecord[0]);
					}// skip record if transformation returned false
				}
				yield();
			} catch (IOException ex) {
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
				closeAllOutputPorts();
				return;
			} catch (Exception ex) {
				resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
				resultCode = Node.RESULT_FATAL_ERROR;
				//closeAllOutputPorts();
				return;
			}
		}
		// signal end of record stream to transformation function
		transformation.finished();
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		resultCode = Node.RESULT_OK;
	}


	/**
	 *  Initialization of component
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
		// do we have transformation object directly specified or shall we create it ourselves
		if (transformation == null) {
			if (transformClassName != null) {
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
			} else {
				System.out.print(" (compiling dynamic source) ");
				// use DynamicJavaCode to instantiate transformation class
				Object transObject = dynamicTransformCode.instantiate();
				if (transObject instanceof RecordTransform) {
					transformation = (RecordTransform) transObject;
				} else {
					throw new ComponentNotReadyException("Provided transformation class doesn't implement RecordTransform.");
				}

			}

		}
		// init transformation
		DataRecordMetadata inMetadata[]={ getInputPort(READ_FROM_PORT).getMetadata()};
		DataRecordMetadata outMetadata[]={getOutputPort(WRITE_TO_PORT).getMetadata()};
		if (!transformation.init(transformationParameters,inMetadata,outMetadata)) {
			throw new ComponentNotReadyException("Error when initializing reformat function !");
		}
	}

	
    /**
     * @param transformationParameters The transformationParameters to set.
     */
    public void setTransformationParameters(Properties transformationParameters) {
        this.transformationParameters = transformationParameters;
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
		DynamicJavaCode dynaTransCode;
		Reformat reformat;

		try {
			//if transform class defined (as an attribute) use it first
			if (xattribs.exists("transformClass")) {
				reformat= new Reformat(xattribs.getString("id"),
						xattribs.getString("transformClass"));
			} else {
				// do we have child node wich Java source code ?
				dynaTransCode = DynamicJavaCode.fromXML(nodeXML);
				if (dynaTransCode == null) {
					throw new RuntimeException("Can't create DynamicJavaCode object - source code not found !");
				}
				reformat = new Reformat(xattribs.getString("id"), dynaTransCode);
			}
			reformat.setTransformationParameters(xattribs.attributes2Properties(new String[]{"transformClass"}));
			
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
			return null;
		}
		return reformat;
	}


	/**
	 *  Checks that component is configured properly
	 *
	 * @return    Description of the Return Value
	 */
	public boolean checkConfig() {
		return true;
	}

	public String getType(){
		return COMPONENT_TYPE;
	}
}


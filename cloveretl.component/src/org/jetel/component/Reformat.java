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
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.DynamicJavaCode;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Reformat Component</h3>
 *
 * <!-- Changes / reformats the data between INPUT/OUTPUT ports.
 *  In general, it transforms between 1 input and several output ports.
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
 * <td>Changes / reformats data record between one INPUT and several OUTPUT ports.<br>
 *  This component is only a wrapper around transformation class implementing
 *  <i>org.jetel.component.RecordTransform</i> interface. The method <i>transform</i>
 *  is called for every record passing through this component.<br></td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]...[n] - output records (on several output ports)</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"REFORMAT"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>libraryPath</b><br><i>optional</i></td><td>name of Java library file (.jar,.zip,...) where
 *  to search for class to be used for transforming data specified in <tt>transformClass<tt> parameter.</td></tr>
 *  <tr><td><b>transformClass</b></td><td>name of the class to be used for transforming data</td>
 *  </tr>
 *  <tr><td><b>transform</b></td><td>contains definition of transformation in internal clover format </td></tr>
 *  <tr><td><b>javaSource</b></td><td>java source code implementation of transformation included direct into node definition</td></tr>
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
 * &lt;attr name="javaSource"&gt;
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
 * &lt;/attr&gt;
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

	private static final String XML_TRANSFORMCLASS_ATTRIBUTE = "transformClass";
	private static final String XML_TRANSFORM_ATTRIBUTE = "transform";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "REFORMAT";

	private final static int READ_FROM_PORT = 0;

    private String transform = null;
	private String transformClass = null;
	private DynamicJavaCode dynamicTransformCode = null;
	private RecordTransform transformation = null;

	private Properties transformationParameters = null;
	
	static Log logger = LogFactory.getLog(Reformat.class);

    /**
	 *Constructor for the Reformat object
	 *
	 * @param  id              unique identification of component
	 * @param  transformClass  Name of transformation CLASS (e.g. org.jetel.test.DemoTrans)
	 */
	public Reformat(String id, String transform, String transformClass) {
		super(id);
        this.transform = transform;
        this.transformClass = transformClass;
	}

	/**
	 *  Main processing method for the reformat object
	 *
	 * @since    April 4, 2002
	 */
	public void run() {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord inRecord[] = {new DataRecord(inPort.getMetadata())};
		int numOutputPorts=getOutPorts().size();
		DataRecord outRecord[] = new DataRecord[numOutputPorts]; 
		
		inRecord[0].init();
		// initialize output ports
		for (int i=0;i<numOutputPorts;i++){
		    outRecord[i]=new DataRecord(getOutputPort(i).getMetadata());
		    outRecord[i].init();
		}
		
		// MAIN PROCESSING LOOP
		int outPort;
		while (inRecord[0] != null && runIt) {
			try {
				inRecord[0] = readRecord(READ_FROM_PORT, inRecord[0]);
				if (inRecord[0] != null) {
					if (transformation.transform(inRecord, outRecord)) {
					     for(outPort=0;outPort<numOutputPorts;outPort++){
					         writeRecord(outPort, outRecord[outPort]);
					     }
					}// skip record if transformation returned false
				}
				SynchronizeUtils.cloverYield();
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
		try{
		    transformation.finished();
		}catch (Exception ex) {
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			//closeAllOutputPorts();
			return;
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
	 *  Initialization of component
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		// test that we have at least one input port and one output
		if (inPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one input port has to be defined!");
		} else if (outPorts.size() < 1) {
			throw new ComponentNotReadyException("At least one output port has to be defined!");
		}
        
        //create input metadata
        DataRecordMetadata inMetadata[] = {getInputPort(READ_FROM_PORT).getMetadata()};

        //create output metadata
        int outPortsNum = getOutPorts().size();
        DataRecordMetadata outMetadata[] = new DataRecordMetadata[outPortsNum];
        for(int i = 0; i < outPortsNum; i++) {
            outMetadata[i] = getOutputPort(i).getMetadata();
        }

		//create instance of record transformation
        try {
            transformation = RecordTransformFactory.createTransform(transform, transformClass, this, inMetadata, outMetadata, transformationParameters);
        } catch(Exception e) {
            throw new ComponentNotReadyException(this, e);
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
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		if (transformClass != null) {
			xmlElement.setAttribute(XML_TRANSFORMCLASS_ATTRIBUTE, transformClass);
		} else {
//        comment by Martin Zatopek - must be changed (now I removing TransformationGraph singleton)
//			Document doc = TransformationGraphXMLReaderWriter.getReference().getOutputXMLDocumentReference();
//			Text text = doc.createTextNode(this.dynamicTransformCode.getSourceCode());
//			xmlElement.appendChild(text);
		}
		
		Enumeration propertyAtts = transformationParameters.propertyNames();
		while (propertyAtts.hasMoreElements()) {
			String attName = (String)propertyAtts.nextElement();
			xmlElement.setAttribute(attName,transformationParameters.getProperty(attName));
		}
				
	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           May 21, 2002
	 */
	   @Override public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		Reformat reformat;

		try {
            reformat = new Reformat(
                            xattribs.getString(XML_ID_ATTRIBUTE),
                            xattribs.getString(XML_TRANSFORM_ATTRIBUTE, null), 
                            xattribs.getString(XML_TRANSFORMCLASS_ATTRIBUTE, null));
			reformat.setTransformationParameters(xattribs.attributes2Properties(new String[]{XML_TRANSFORMCLASS_ATTRIBUTE}));
			
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
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


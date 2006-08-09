/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-05  Javlin Consulting <info@javlinconsulting.cz>
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DelimitedDataFormatterNIO;
import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.parser.DelimitedDataParserNIO;
import org.jetel.data.parser.FixLenDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;

/**
 *  <h3>System Execute Component</h3> 
 *  <!-- This component executes system command.-->
 * 
 *  <table border="1">
 *
 *    <th>
 *      Component:
 *    </th>
 *    <tr><td>
 *        <h4><i>Name:</i> </h4></td><td>SystemExecute</td>
 *    </tr>
 *    <tr><td><h4><i>Category:</i> </h4></td><td></td>
 *    </tr>
 *    <tr><td><h4><i>Description:</i> </h4></td>
 *      <td>
 *  This component executes system command.<br>
 *      </td>
 *    </tr>
 *    <tr><td><h4><i>Inputs:</i> </h4></td>
 *    <td>
 *        [0]- input records for system command<br>
 *    </td></tr>
 *    <tr><td> <h4><i>Outputs:</i> </h4>
 *      </td>
 *      <td>
 *        [0] - output stream from system command
 *      </td></tr>
 *    <tr><td><h4><i>Comment:</i> </h4>
 *      </td>
 *      <td></td>
 *    </tr>
 *  </table>
 *  <br>
 *  <table border="1">
 *    <th>XML attributes:</th>
 *    <tr><td><b>type</b></td><td>SYS_EXECUTE</td></tr>
 *    <tr><td><b>command</b></td><td> command to be execute by system</td></tr>
 *    <tr><td><b>errorLinesNumber</b></td><td> number of lines that are print
 *    	 out if command finishes with errors</td></tr>
 *    </table>
 *    <h4>Example:</h4> <pre>&lt;Node id="SYS_EXECUTE0" type="SYS_EXECUTE"&gt;
 * &lt;attr name="errorLinesNumber"&gt;3&lt;/attr&gt;
 * &lt;attr name="command"&gt;rm test.txt &lt;/attr&gt;
 * &lt;/Node&gt;</pre>
 *
 * @author avackova
 *
 */
public class SystemExecute extends Node{
	
	private static final String XML_COMMAND_ATTRIBUTE = "command";
	private static final String XML_ERROR_LINES_ATTRIBUTE = "errorLinesNumber";
	public final static String COMPONENT_TYPE = "SYS_EXECUTE";

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	
	private final static int ERROR_LINES=2;

	private String command;
	private int errorLinesNumber;
	private int exitValue;
	private Parser parser;
	private Formatter formatter;
	
	static Log logger = LogFactory.getLog(SystemExecute.class);
	
	public SystemExecute(String id,String command) {
		super(id);
		this.command=command;
		errorLinesNumber=2;
	}

	public SystemExecute(String id,String command,int errorLinesNumber) {
		super(id);
		this.command=command;
		this.errorLinesNumber=errorLinesNumber;
	}

	public void run() {
		resultCode = Node.RESULT_OK;
		//Creating and initializing record from input port
		DataRecord in_record=null;
		InputPort inPort = getInputPort(INPUT_PORT);
		//If there is input port read metadadata and initialize in_record
		if (inPort!=null) {
			DataRecordMetadata meta=inPort.getMetadata();
			in_record = new DataRecord(meta);
			in_record.init();
			if (meta.getRecType()==DataRecordMetadata.DELIMITED_RECORD) {
				formatter=new DelimitedDataFormatterNIO();
			}else {
				formatter=new FixLenDataFormatter();
			}
		}else{
			formatter=null;
		}
		//Creating and initializing record to otput port
		DataRecord out_record=null;
		OutputPort outPort=getOutputPort(OUTPUT_PORT);
		//If there is output port read metadadata and initialize in_record
		if (outPort!=null) {
			DataRecordMetadata meta=outPort.getMetadata();
			out_record= new DataRecord(meta);
			out_record.init();
			if (meta.getRecType()==DataRecordMetadata.DELIMITED_RECORD) {
				parser=new DelimitedDataParserNIO();
			}else {
				parser=new FixLenDataParser();
			}
		}else{
			parser=null;
		}

		
		Runtime r=Runtime.getRuntime();
		
		try{
			Process process=r.exec(command);
			OutputStream process_in=process.getOutputStream();
			InputStream process_out=process.getInputStream();
			InputStream process_err=process.getErrorStream();
			// If there is input port read records and write them to input stream of the process
			GetData getData=null; 
			if (inPort!=null) {
                formatter.open(process_in,getInputPort(INPUT_PORT).getMetadata());
                getData=new GetData(inPort, in_record, formatter);
				getData.start();
			}
			//If there is output port read output from process and send it to output ports
			SendData sendData=null;
			if (outPort!=null){
                parser.open(process_out, getOutputPort(OUTPUT_PORT).getMetadata());
                sendData=new SendData(outPort,out_record,parser);
				//send all out_records to output ports
				sendData.start();
			}
			BufferedReader err=new BufferedReader(new InputStreamReader(process_err));
			String line;
			StringBuffer errmes=new StringBuffer();
			int i=0;
			while (((line=err.readLine())!=null)&&i++<Math.max(errorLinesNumber,ERROR_LINES)){
				if (i<=errorLinesNumber)
					logger.debug(line);
				if (i<=ERROR_LINES)
					errmes.append(line+"\n");
			}
			if (ERROR_LINES<i) errmes.append(".......\n");
			err.close();
			process_err.close();
			resultMsg=errmes.toString();
            exitValue=process.waitFor();
            if (getData!=null || sendData!=null){
            	boolean stoped=false;
				if (getData!=null && getData.getResultCode()==Node.RESULT_RUNNING) {
					getData.stop_it();
					stoped=true;
				}
				if (sendData!=null && sendData.getResultCode()==Node.RESULT_RUNNING) {
					sendData.stop_it();
					stoped=true;
				}
				if (stoped) Thread.sleep(10000);
				if (getData!=null && getData.getResultCode()==Node.RESULT_RUNNING) getData.interrupt();
				if (sendData!=null && sendData.getResultCode()==Node.RESULT_RUNNING) sendData.interrupt();
				if (getData!=null && getData.getResultCode()!=Node.RESULT_OK) {
					resultMsg = resultMsg + "\n" + getData.getResultMsg();
					resultCode = Node.RESULT_ERROR;
				}
				if (sendData!=null && sendData.getResultCode()!=Node.RESULT_OK) {
					resultMsg = resultMsg + "\n" + sendData.getResultMsg();
					resultCode = Node.RESULT_ERROR;
				}
	         }
		}catch(IOException ex){
			ex.printStackTrace();
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
		}catch(Exception ex){
		    ex.printStackTrace();
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			return;
		}
		broadcastEOF();
		if (!runIt) {
			resultMsg = resultMsg + "\n" + "STOPPED";
			resultCode=Node.RESULT_ERROR;
		}
		if (exitValue!=0){
			resultCode = Node.RESULT_ERROR;
		}
		if (resultCode==Node.RESULT_OK){
			resultMsg = "OK";
		}
	}

	public void init() throws ComponentNotReadyException {
		if (getInPorts().size()>1) 
			throw new ComponentNotReadyException(getId() + ": too many input ports");
		if (getOutPorts().size()>1) 
			throw new ComponentNotReadyException(getId() + ": too many otput ports");
	}

	public String getType(){
		return COMPONENT_TYPE;
	}

	public boolean checkConfig() {
		return true;
	}

	public static Node fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		try {
			return new SystemExecute(xattribs.getString(XML_ID_ATTRIBUTE),xattribs.getString(XML_COMMAND_ATTRIBUTE),xattribs.getInteger(XML_ERROR_LINES_ATTRIBUTE,2));
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
	}
	
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_COMMAND_ATTRIBUTE,command);
		xmlElement.setAttribute(XML_ERROR_LINES_ATTRIBUTE,String.valueOf(errorLinesNumber));
	}
	

	private static class GetData extends Thread {

		InputPort inPort;
		DataRecord in_record;
		Formatter formatter;
		String resultMsg=null;
		int resultCode=Node.RESULT_OK;
        volatile boolean runIt;
	
		
		GetData(InputPort inPort,DataRecord in_record,Formatter formatter){
			super();
			this.in_record=in_record;
			this.inPort=inPort;
			this.formatter=formatter;
			runIt=true;
		}
		
		public void stop_it(){
			runIt=false;	
		}
		
		public void run() {
           resultCode=Node.RESULT_RUNNING;
			try{
				while ((( in_record=inPort.readRecord(in_record))!= null ) && runIt) {
					formatter.write(in_record);
                    SynchronizeUtils.cloverYield();
				}
				formatter.close();
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}catch(InterruptedException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}
           resultCode=Node.RESULT_OK;
		}

		public int getResultCode() {
			return resultCode;
		}

		public String getResultMsg() {
			return resultMsg;
		}
	}
	
	private static class SendData extends Thread {

		DataRecord out_record;
		OutputPort outPort;
		Parser parser;
		String resultMsg=null;
		int resultCode=Node.RESULT_OK;
		volatile boolean runIt;
		
		SendData(OutputPort outPort,DataRecord out_record,Parser parser){
			super();
			this.out_record=out_record;
			this.outPort=outPort;
			this.parser=parser;
			this.runIt=true;
		}
		
		public void stop_it(){
			runIt=false;	
		}
		
		public void run() {
            resultCode=Node.RESULT_RUNNING;
			try{
				while (((out_record = parser.getNext(out_record)) != null) && runIt) {
					//broadcast the record to all connected Edges
					outPort.writeRecord(out_record);
					SynchronizeUtils.cloverYield();
				}
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}catch(Exception ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}
            resultCode=Node.RESULT_OK;
		}

        /**
         * @return Returns the resultCode.
         */
        public int getResultCode() {
            return resultCode;
        }

		public String getResultMsg() {
			return resultMsg;
		}
	}
	
}



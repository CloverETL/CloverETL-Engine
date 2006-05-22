package org.jetel.component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;

public class SystemExecute extends Node{
	
	private static final String XML_COMMAND = "Command";
	public final static String COMPONENT_TYPE = "SYS_EXECUTE";

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;

	private String command;
	private int exitValue;
	private Parser parser;
	private Formatter formatter;
	
	private class GetData extends Thread {

		InputPort inPort;
		DataRecord in_record;
		OutputStream p_in; 
		
		GetData(InputPort inPort,DataRecord in_record,OutputStream p_in){
			super();
			this.in_record=in_record;
			this.inPort=inPort;
			this.p_in=p_in;
		}
		
		public void run() {
			try{
				while ((( in_record=inPort.readRecord(in_record))!= null ) && runIt) {
					formatter.write(in_record);
				}
				formatter.close();
				p_in.close();	
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}catch(InterruptedException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}
		}
	}

	private class SendData extends Thread {

		DataRecord out_record;
		
		SendData(DataRecord out_record){
			super();
			this.out_record=out_record;
		}
		
		public void run() {
			try{
				while (((out_record = parser.getNext(out_record)) != null) && runIt) {
					//broadcast the record to all connected Edges
					writeRecordBroadcast(out_record);
					SynchronizeUtils.cloverYield();
				}
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}catch(Exception ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}
		}
	}
	
	public SystemExecute(String id,String command) {
		super(id);
		this.command=command;
	}

	public void run() {
		//Creating and initializing record from input port
		DataRecord in_record=null;
		InputPort inPort = getInputPort(INPUT_PORT);
		//If there is input port read metadadata and initialize in_record
		try{
			DataRecordMetadata meta=inPort.getMetadata();
			in_record = new DataRecord(meta);
			in_record.init();
			if (meta.getRecType()==DataRecordMetadata.DELIMITED_RECORD) {
				formatter=new DelimitedDataFormatterNIO();
			}else {
				formatter=new FixLenDataFormatter();
			}
		}catch(NullPointerException e){
			formatter=null;
		}
		//Creating and initializing record to otput port
		DataRecord out_record=null;
		//If there is output port read metadadata and initialize in_record
		try {
			DataRecordMetadata meta=getOutputPort(OUTPUT_PORT).getMetadata();
			out_record= new DataRecord(meta);
			out_record.init();
			if (meta.getRecType()==DataRecordMetadata.DELIMITED_RECORD) {
				parser=new DelimitedDataParserNIO();
			}else {
				parser=new FixLenDataParser();
			}
		}catch(NullPointerException e){
			parser=null;
		}

		final int inPorts_size=inPorts.size();
		final int outPorts_size=outPorts.size();
		
		Runtime r=Runtime.getRuntime();
		
		try{
			Process p=r.exec(command);
			OutputStream p_in=p.getOutputStream();
			InputStream p_out=p.getInputStream();
			// If there is input port read records and write them to input stream of the process
			if (inPorts_size>0) {
				formatter.open(p_in,getInputPort(INPUT_PORT).getMetadata());
				new GetData(inPort, in_record, p_in).start();
			}
			//If there is output port read output from process and send it to output ports
			if (outPorts_size>0){
				parser.open(p_out, getOutputPort(OUTPUT_PORT).getMetadata());
				//send all out_records to output ports
				SendData s=new SendData(out_record);
				s.start();
				s.join();
			}
		}catch(IOException ex){
			resultMsg = ex.getMessage();
			resultCode = Node.RESULT_ERROR;
		}catch(Exception ex){
		    ex.printStackTrace();
			resultMsg = ex.getClass().getName()+" : "+ ex.getMessage();
			resultCode = Node.RESULT_FATAL_ERROR;
			return;
		}
		broadcastEOF();
		if (runIt) {
			resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		if (exitValue==0)
			resultCode = Node.RESULT_OK;
		else
			resultCode=Node.RESULT_ERROR;
	}

	public void init() throws ComponentNotReadyException {
		if (inPorts.size()>1) 
			throw new ComponentNotReadyException(getID() + ": too many input ports");
	}

	public String getType(){
		return COMPONENT_TYPE;
	}

	public boolean checkConfig() {
		return true;
	}

	public static Node fromXML(org.w3c.dom.Node nodeXML) {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML);
		try {
			return new SystemExecute(xattribs.getString(Node.XML_ID_ATTRIBUTE),xattribs.getString(XML_COMMAND));
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
	}

}



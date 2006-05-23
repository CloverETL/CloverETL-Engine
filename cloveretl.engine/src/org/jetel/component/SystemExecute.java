package org.jetel.component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.jetel.graph.OutputPort;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;

public class SystemExecute extends Node{
	
	private static final String XML_COMMAND_ATTRIBUTE = "command";
	public final static String COMPONENT_TYPE = "SYS_EXECUTE";

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;

	private String command;
	private int exitValue;
	private Parser parser;
	private Formatter formatter;
	
	public SystemExecute(String id,String command) {
		super(id);
		this.command=command;
	}

	public void run() {
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
			Process p=r.exec(command);
			OutputStream p_in=p.getOutputStream();
			InputStream p_out=p.getInputStream();
			InputStream p_err=p.getErrorStream();
			// If there is input port read records and write them to input stream of the process
			GetData gd=null; 
			if (inPort!=null) {
                formatter.open(p_in,getInputPort(INPUT_PORT).getMetadata());
                gd=new GetData(inPort, in_record, formatter);
				if (outPort!=null) gd.start();
				else gd.run();
			}
			//If there is output port read output from process and send it to output ports
			SendData sd=null;
			if (outPort!=null){
                parser.open(p_out, getOutputPort(OUTPUT_PORT).getMetadata());
                sd=new SendData(outPort,out_record,parser);
				//send all out_records to output ports
				sd.run();
			}
            
            
		
            exitValue=p.waitFor();
			if (exitValue!=0){
				BufferedReader err=new BufferedReader(new InputStreamReader(p_err));
				String line="";
				while ((line=err.readLine())!=null)
					resultMsg+=line+"\n";
				err.close();
			}
			if (gd.isAlive()) {
				gd.stop_it();
				sleep(10000);
				if (gd.isAlive()) gd.interrupt();
			}
			if (!(gd.resultCode==Node.RESULT_OK)) {
				resultMsg = gd.resultMsg;
				resultCode = Node.RESULT_ERROR;
			}
			if (!(sd.resultCode==Node.RESULT_OK)) {
				resultMsg = sd.resultMsg;
				resultCode = Node.RESULT_ERROR;
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
		if (runIt) {
			if (exitValue==0) resultMsg = "OK";
		} else {
			resultMsg = "STOPPED";
		}
		if (exitValue==0)
			resultCode = Node.RESULT_OK;
		else {
			resultCode=Node.RESULT_ERROR;
			System.out.print(resultMsg);
		}
	}

	public void init() throws ComponentNotReadyException {
		if (getInPorts().size()>1) 
			throw new ComponentNotReadyException(getID() + ": too many input ports");
		if (getOutPorts().size()>1) 
			throw new ComponentNotReadyException(getID() + ": too many otput ports");
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
			return new SystemExecute(xattribs.getString(Node.XML_ID_ATTRIBUTE),xattribs.getString(XML_COMMAND_ATTRIBUTE));
		} catch (Exception ex) {
			System.err.println(COMPONENT_TYPE + ":" + ((xattribs.exists(XML_ID_ATTRIBUTE)) ? xattribs.getString(Node.XML_ID_ATTRIBUTE) : " unknown ID ") + ":" + ex.getMessage());
			return null;
		}
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
		}
	}
	
	private static class SendData extends Thread {

		DataRecord out_record;
		OutputPort outPort;
		Parser parser;
		String resultMsg=null;
		int resultCode;
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
	}
	
}



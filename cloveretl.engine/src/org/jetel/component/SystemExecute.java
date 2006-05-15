package org.jetel.component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.DelimitedDataParserNIO;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.SynchronizeUtils;

public class SystemExecute extends Node{
	
	private static final String XML_COMMAND = "Command";
	public final static String COMPONENT_TYPE = "SYS_EXECUTE";

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;

	private String command;
	private int exitValue;
	private DelimitedDataParserNIO parser;
	 
	public SystemExecute(String id,String command) {
		super(id);
		this.command=command;
	}

	public void run() {
		
		Runtime r=Runtime.getRuntime();
		if (runIt) 
			try{
				Process p=r.exec(command);
				if (outPorts.size()>0){
					DataRecord record = new DataRecord(getOutputPort(OUTPUT_PORT).getMetadata());
					record.init();
					parser=new DelimitedDataParserNIO();
//					File tmp=File.createTempFile("in_",null);
//					RandomAccessFile t=new RandomAccessFile(tmp,"rw");
//					BufferedReader br = new BufferedReader( new InputStreamReader( p.getInputStream() ) );
//					String line;
//					while ((line=br.readLine())!=null){
//						t.writeBytes(line+"\n");
//					}
//					t.close();
					parser.open(p.getInputStream(), getOutputPort(OUTPUT_PORT).getMetadata());
					while (((record = parser.getNext(record)) != null) && runIt) {
						//broadcast the record to all connected Edges
						writeRecordBroadcast(record);
						SynchronizeUtils.cloverYield();
					}
				}
				exitValue=p.waitFor();
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}catch(InterruptedException ex){
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



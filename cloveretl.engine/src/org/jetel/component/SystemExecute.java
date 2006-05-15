package org.jetel.component;

import java.io.IOException;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.util.ComponentXMLAttributes;
import org.w3c.dom.Element;

public class SystemExecute extends Node{
	
	private static final String XML_COMMAND = "Command";
	public final static String COMPONENT_TYPE = "SYS_EXECUTE";

	private String command;
	private int exitValue;

	public SystemExecute(String id,String command) {
		super(id);
		this.command=command;
	}

	public void run() {
		
		Runtime r=Runtime.getRuntime();
		if (runIt) 
			try{
				Process p=r.exec(command);
				exitValue=p.waitFor();
			}catch(IOException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
			}catch(InterruptedException ex){
				resultMsg = ex.getMessage();
				resultCode = Node.RESULT_ERROR;
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



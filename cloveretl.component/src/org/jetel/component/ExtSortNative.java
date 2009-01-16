package org.jetel.component;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

public class ExtSortNative extends Node {

	public final static String COMPONENT_TYPE = "EXT_SORT_NATIVE";

	DataRecord inRecord;
	InputPortDirect inPortDirect;
	String sortCommand;
	
	public void setSortCommand(String sortCommand) {
		this.sortCommand = sortCommand;
	}

	public ExtSortNative(String id) {
		super(id);
	}

	@Override
	public Result execute() throws Exception {

		inPortDirect = getInputPortDirect(0);
		inRecord = new DataRecord(inPortDirect.getMetadata());
		inRecord.init();
		DataRecord tmpRecord;
		File tmpFile = File.createTempFile("clover_native_sort_in", ".dat");
		System.out.println("Temp input file is " + tmpFile.getPath());
		//tmpFile.deleteOnExit();
		
		Formatter out = new DelimitedDataFormatter(); 
		out.init(inPortDirect.getMetadata());
		FileOutputStream fos = new FileOutputStream(tmpFile); 
		out.setDataTarget(fos);
		
		do {
			
			tmpRecord = inPortDirect.readRecord(inRecord);
			if (tmpRecord == null) {
				break;
			}
			
			out.write(inRecord);
			SynchronizeUtils.cloverYield();

		} while(runIt);
		out.close();
		fos.flush();
		fos.close();
		Thread.sleep(2000);
		// ted mame naplneny tmpFile

		File tmpFileOut = File.createTempFile("clover_native_sort_out", ".dat");
		System.out.println("Temp input file is " + tmpFile.getPath());
		
		// pustime na nej sort
		String sortCommand = getSortCommand(tmpFile.getPath(), tmpFileOut.getPath());

		System.out.println("Running native sort: " + sortCommand);
		Process p = Runtime.getRuntime().exec(sortCommand);
		InputStream eis = p.getErrorStream();
		while(eis.available() > 0) {
			System.err.write(eis.read());
		}
		p.waitFor();
		System.out.println("process retcode: " + p.exitValue());
		// lets read the file
		Parser in = new DelimitedDataParser();
		in.init(inPortDirect.getMetadata());
		in.setDataSource(new FileInputStream(tmpFileOut));
		while((tmpRecord = in.getNext()) != null) {
			writeRecordBroadcast(tmpRecord);
		}
		in.close();
		
		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;

	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();
	}

	
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		ExtSortNative sort = null;
		try {
			sort = new ExtSortNative(xattribs.getString(XML_ID_ATTRIBUTE));
		} catch (AttributeNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return sort;
	}
	
	public String getSortCommand(String inFile, String outFile) throws Exception {
		String os = System.getProperty("os.name").toLowerCase();
		if (os.contains("windows")) {
			return "sort \"" + inFile + "\" /o \"" + outFile + "\"";
		} else if (os.contains("linux") || os.contains("solaris")) {
			return "/usr/bin/sort " + inFile + " --output=" + outFile;
		} else {
			throw new Exception("Cannot determine supported host OS.");
		}
	}
	
}

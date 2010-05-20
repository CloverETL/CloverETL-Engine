/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

import org.jetel.component.DataReader;
import org.jetel.component.DataWriter;
import org.jetel.component.ExtSort;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.main.runGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

public class testGraphSort {
	
	private final static String PARAMETER_FILE = "params.txt"; 
	private final static String PLUGINS_PROPERTY = "plugins";
	private final static String DATA_FILE_PROPERTY = "dataFile";
	private final static String OUTPUT_FILE_PROPERTY = "outputFile";
	private final static String METADATA_PROPERTY = "metadata";
	private final static String KEY_PROPERTY = "sortKey";

	private final static String[] ARGS = {DATA_FILE_PROPERTY, OUTPUT_FILE_PROPERTY, METADATA_PROPERTY, KEY_PROPERTY, PLUGINS_PROPERTY};
	
	private final static int DATA_FILE_PROPERTY_INDEX = 0;
	private final static int OUTPUT_FILE_PROPERTY_INDEX = 1;
	private final static int METADATA_PROPERTY_INDEX = 2;
	private final static int KEY_PROPERTY_INDEX = 3;
	private final static int PLUGINS_PROPERTY_INDEX = 4;

	private static final Phase _PHASE_1=new Phase(1);
	private static final Phase _PHASE_2=new Phase(2);

	public static void main(String args[]){
	
		Properties arguments = new Properties();
		if ((new File(PARAMETER_FILE)).exists()) {
			try {
				arguments.load(new FileInputStream(PARAMETER_FILE));
			} catch (FileNotFoundException e) {
				//do nothing: we checked it
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
    
		String[] arg = new String[ARGS.length];
		
		for (int i = 0; i < arg.length; i++){
			if (args.length > i) {
				arg[i] = args[i];
			}else{
				arg[i] = arguments.getProperty(ARGS[i]);
				if (i < 3 && arg[i] == null) {
					System.out.println("Required argument " + ARGS[i] + " not found");
					System.out.println("Example graph which sorts input data according to specified key.");
					System.out.println("The sortKey must be a name of field(or comma delimited fields) from input data.");
					System.out.println("Usage: testGraphSort <input data filename> <output sorted filename> <metadata filename> <sortKey> [<plugin directory> <default properties file>]");
					System.exit(1);
				}
			}
		}

	//initialization; must be present
	EngineInitializer.initEngine(arg[PLUGINS_PROPERTY_INDEX], null, null);

	System.out.println("**************** Input parameters: ****************");
	System.out.println("Input file: "+arg[DATA_FILE_PROPERTY_INDEX]);
	System.out.println("Output file: "+arg[OUTPUT_FILE_PROPERTY_INDEX]);
	System.out.println("Input Metadata: "+arg[METADATA_PROPERTY_INDEX]);
	System.out.println("Key: "+arg[KEY_PROPERTY_INDEX]);
	System.out.println("Plugins directory: "+ arg[PLUGINS_PROPERTY_INDEX]);
	System.out.println("***************************************************");
	
	DataRecordMetadata metadataIn;
	DataRecordMetadataXMLReaderWriter reader=new DataRecordMetadataXMLReaderWriter();
		
	try{
		metadataIn=reader.read(new FileInputStream(arg[METADATA_PROPERTY_INDEX]));
	}catch(IOException ex){
		System.err.println("Error when reading metadata!!");
		throw new RuntimeException(ex);
	}
		
	if (metadataIn==null){
		throw new RuntimeException("No INPUT metadata");
	}
	
	// create Graph + Node + 2 connections (edges)
	TransformationGraph graph = new TransformationGraph();
	Edge inEdge=new Edge("InEdge",metadataIn);
	Edge outEdge=new Edge("OutEdge",metadataIn);
	
	Node nodeRead=new DataReader("DataParser",arg[DATA_FILE_PROPERTY_INDEX]);
	String[] sortKeys=arg[KEY_PROPERTY_INDEX].split(",");
	Node nodeSort=new ExtSort("Sorter",sortKeys, true);
	Node nodeWrite=new DataWriter("DataWriter",arg[OUTPUT_FILE_PROPERTY_INDEX],"UTF-8",false);
	
	// assign ports (input & output)
	nodeRead.addOutputPort(0,inEdge);
	nodeSort.addInputPort(0,inEdge); 
	nodeSort.addOutputPort(0,outEdge);
	nodeWrite.addInputPort(0,outEdge);
	
	
	// add	Edges & Nodes & Phases to graph
	try {
		graph.addPhase(_PHASE_1);
		_PHASE_1.addNode(nodeRead);
		_PHASE_1.addNode(nodeSort);
		graph.addPhase(_PHASE_2);
		_PHASE_2.addNode(nodeWrite);
		graph.addEdge(inEdge);
		graph.addEdge(outEdge);
	}catch (GraphConfigurationException ex){
		ex.printStackTrace();
	}
	
	//prepare runtime parameters - JMX is turned off
    GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
    runtimeContext.setUseJMX(false);
    
	Future<Result> result;
	try{
		result = runGraph.executeGraph(graph, runtimeContext);
		while (result.isDone()) {;}
		if (!result.get().equals(Result.FINISHED_OK)){
			System.out.println("Failed graph execution!\n");
			return;		
		}
	}catch (Exception e) {
		System.out.println("Failed graph execution!\n" + e.getMessage());
		return;		
	}
	
	}
} 



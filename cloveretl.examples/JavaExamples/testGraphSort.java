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

/**
 * Program parameters:
 * -plugins	pluginsDirectory		CloverETL plugins directory
 * -config propertiesFile			load default engine properties from specified file
 * -dataFile 						file with input data to parse
 * -outputFile						resulting output file
 * -sotrtKey						sort key (sequence of field names followed by (a) or (d)  meaning that the field 
 * 									is sorted in ascending or descending order, respectively. 
 * 									The individual expressions are separated by :;|  {colon, semicolon, pipe})
 * -metadataFile					metadata definition file
 * 
 * This example illustrates how to create and run transformation graph from java code.
 * All examples require to provide CloverETL plugins. Plugins directory can be set as program argument or 
 * in params.txt file as plugins parameter or required plugins have to be set on classpath when running the program. 
 * When set in params.txt, the same directory is used for all examples and musn't be set 
 * for each example separately.
 * This examples requires some additional parameters: data file, output file, sort key and metadata. 
 * All the properties are read from params.txt, if they are not set as program arguments.
 *
 */
public class testGraphSort {
	
	private final static String PARAMETER_FILE = "params.txt"; 
	private final static String PLUGINS_PROPERTY = "plugins";
	private final static String PROPERTIES_FILE_PROPERTY = "config";
	private final static String DATA_FILE_PROPERTY = "dataFile";
	private final static String OUTPUT_FILE_PROPERTY = "outputFile";
	private final static String METADATA_PROPERTY = "metadata";
	private final static String KEY_PROPERTY = "sortKey";

	private static final Phase _PHASE_1=new Phase(1);
	private static final Phase _PHASE_2=new Phase(2);

	public static void main(String args[]){
	
		//reading parameters from params.txt file
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
		//overriding requested parameters from program parameters 
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("-" + PLUGINS_PROPERTY)) {
				arguments.setProperty(PLUGINS_PROPERTY, args[++i]);
			}else if (args[i].startsWith("-" +PROPERTIES_FILE_PROPERTY)){
				arguments.setProperty(PROPERTIES_FILE_PROPERTY, args[++i]);
			}else if (args[i].startsWith("-" + DATA_FILE_PROPERTY)){
				arguments.setProperty(DATA_FILE_PROPERTY, args[++i]);
			}else if (args[i].startsWith("-" + OUTPUT_FILE_PROPERTY)){
				arguments.setProperty(OUTPUT_FILE_PROPERTY, args[++i]);
			}else if (args[i].startsWith("-" + KEY_PROPERTY)){
				arguments.setProperty(KEY_PROPERTY, args[++i]);
			}else if (args[i].startsWith("-" + METADATA_PROPERTY)){
				arguments.setProperty(METADATA_PROPERTY, args[++i]);
			}
		}
		
		//checking requested parameters
		boolean missingProperty = false;
		if (!arguments.containsKey(DATA_FILE_PROPERTY)){
			missingProperty = true;
			System.out.println(DATA_FILE_PROPERTY + " property not found");
		}
		if (!arguments.containsKey(OUTPUT_FILE_PROPERTY)){
			missingProperty = true;
			System.out.println(OUTPUT_FILE_PROPERTY + " property not found");
		}
		if (!arguments.containsKey(KEY_PROPERTY)){
			missingProperty = true;
			System.out.println(KEY_PROPERTY + " property not found");
		}
		if (!arguments.containsKey(METADATA_PROPERTY)){
			missingProperty = true;
			System.out.println(METADATA_PROPERTY + " property not found");
		}
		if (missingProperty) {
			System.exit(1);
		}

	System.out.println("**************** Input parameters: ****************");
	System.out.println("Input file: "+ arguments.getProperty(DATA_FILE_PROPERTY));
	System.out.println("Output file: "+ arguments.getProperty(OUTPUT_FILE_PROPERTY));
	System.out.println("Input Metadata: "+ arguments.getProperty(METADATA_PROPERTY));
	System.out.println("Key: "+ arguments.getProperty(KEY_PROPERTY));
	System.out.println("Plugins directory: "+ arguments.getProperty(PLUGINS_PROPERTY));
	System.out.println("Default properties file: "+ arguments.getProperty(PROPERTIES_FILE_PROPERTY));
	System.out.println("***************************************************");
	
	//initialization; must be present
	EngineInitializer.initEngine(arguments.getProperty(PLUGINS_PROPERTY), arguments.getProperty(PROPERTIES_FILE_PROPERTY), null);

	//reading metadata from fmt file
	DataRecordMetadata metadataIn;
	DataRecordMetadataXMLReaderWriter reader=new DataRecordMetadataXMLReaderWriter();
	try{
		metadataIn=reader.read(new FileInputStream(arguments.getProperty(METADATA_PROPERTY)));
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
	
	Node nodeRead=new DataReader("DataParser",arguments.getProperty(DATA_FILE_PROPERTY));
	nodeRead.setName("DataParser");
	String[] sortKeys=arguments.getProperty(KEY_PROPERTY).split(",");
	Node nodeSort=new ExtSort("Sorter",sortKeys, true);
	nodeSort.setName("Sorter");
	Node nodeWrite=new DataWriter("DataWriter",arguments.getProperty(OUTPUT_FILE_PROPERTY),"UTF-8",false);
	nodeWrite.setName("DataWriter");
	
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
    
    //execute graph
	Future<Result> result;
	try{
		result = runGraph.executeGraph(graph, runtimeContext);
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



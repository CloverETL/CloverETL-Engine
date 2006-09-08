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

package javaExamples;
import java.io.FileInputStream;
import java.io.IOException;

import org.jetel.component.ComponentFactory;
import org.jetel.component.DelimitedDataReader;
import org.jetel.component.DelimitedDataWriter;
import org.jetel.component.Sort;
import org.jetel.data.Defaults;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.graph.Edge;
import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

public class testGraphSort {
	
	private static final Phase _PHASE_1=new Phase(1);
	private static final Phase _PHASE_2=new Phase(2);

	public static void main(String args[]){
	
	//initialization; must be present
    Defaults.init();
    ComponentFactory.init();
    
	if (args.length!=4){
		System.out.println("Example graph which sorts input data according to specified key.");
		System.out.println("The key must be a name of field(or comma delimited fields) from input data.");
		System.out.println("Usage: testGraphSort <input data filename> <output sorted filename> <metadata filename> <key>");
		System.exit(1);
	}
	System.out.println("**************** Input parameters: ****************");
	System.out.println("Input file: "+args[0]);
	System.out.println("Output file: "+args[1]);
	System.out.println("Input Metadata: "+args[2]);
	System.out.println("Key: "+args[3]);
	System.out.println("***************************************************");
	
	DataRecordMetadata metadataIn;
	DataRecordMetadataXMLReaderWriter reader=new DataRecordMetadataXMLReaderWriter();
		
	try{
	metadataIn=reader.read(new FileInputStream(args[2]));
	}catch(IOException ex){
		System.err.println("Error when reading metadata!!");
		throw new RuntimeException(ex);
	}
		
	if (metadataIn==null){
		throw new RuntimeException("No INPUT metadata");
	}
	
	// create Graph + Node + 2 connections (edges)
	TransformationGraph graph = new TransformationGraph();
	Edge inEdge=new Edge("In Edge",metadataIn);
	Edge outEdge=new Edge("Out Edge",metadataIn);
	
	Node nodeRead=new DelimitedDataReader("Data Parser",args[0]);
	String[] sortKeys=args[3].split(",");
	Node nodeSort=new Sort("Sorter",sortKeys, true);
	Node nodeWrite=new DelimitedDataWriter("Data Writer",args[1],false);
	
	// add	Edges & Nodes & Phases to graph
	try {
		graph.addEdge(inEdge);
		graph.addEdge(outEdge);
		
		graph.addPhase(_PHASE_1);
		_PHASE_1.addNode(nodeRead);
		_PHASE_1.addNode(nodeSort);
		graph.addPhase(_PHASE_2);
		_PHASE_2.addNode(nodeWrite);
	}catch (GraphConfigurationException ex){
		ex.printStackTrace();
	}
	
	
	// assign ports (input & output)
	nodeRead.addOutputPort(0,inEdge);
	nodeSort.addInputPort(0,inEdge); 
	nodeSort.addOutputPort(0,outEdge);
	nodeWrite.addInputPort(0,outEdge);
	
	
	if(!graph.init()){
		System.err.println("Graph initialization failed !");
		return;
	}
	
	// output graph layout
	//graph.dumpGraphConfiguration();
	
	
	if (!graph.run()){ // start all Nodes (each node is one thread)
		System.out.println("Failed starting all nodes!");
		return;		
	}
		
	}
	
} 



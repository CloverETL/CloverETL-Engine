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

package org.jetel.test;
import java.io.*;
import org.jetel.metadata.*;
import org.jetel.data.*;
import org.jetel.graph.*;
import org.jetel.component.*;

public class testGraphSort {

	public static void main(String args[]){
		
	DataRecord record;

	int counter=0;
	
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
	TransformationGraph graph= TransformationGraph.getReference();
	Edge inEdge=new Edge("In Edge",metadataIn);
	Edge outEdge=new Edge("Out Edge",metadataIn);
	
	Node nodeRead=new DelimitedDataReaderNIO("Data Parser",args[0]);
	String[] sortKeys=args[3].split(",");
	Node nodeSort=new Sort("Sorter",sortKeys, true);
	Node nodeWrite=new DelimitedDataWriterNIO("Data Writer",args[1],false);
	
	// add	Edges & Nodes to graph
	graph.addEdge(inEdge);
	graph.addEdge(outEdge);
	
	graph.addNode(nodeRead);
	graph.addNode(nodeSort);
	graph.addNode(nodeWrite);
	
	
	// assign ports (input & output)
	nodeRead.addOutputPort(inEdge);
	nodeSort.addInputPort(inEdge);
	nodeSort.addOutputPort(outEdge);
	nodeWrite.addInputPort(outEdge);
	
	
	if(!graph.init(System.out)){
		System.err.println("Graph initialization failed !");
		return;
	}
	if (!graph.run()){ // start all Nodes (each node is one thread)
		System.out.println("Failed starting all nodes!");
		return;		
	}
		
	}
	
} 



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


import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.jetel.component.ComponentFactory;
import org.jetel.data.Defaults;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.main.runGraph;
import org.jetel.plugin.Plugins;

public class testXMLGraph{

	public static void main(String args[]){
		
		if (args.length<1){
			System.out.println("Usage: testXMLGraph <graph file name> <plug-ins directory>");
		}
		
		FileInputStream in;
        
        //initialization; must be present
		//initialization; must be present
		if (args.length == 2) {
			runGraph.initEngine(args[1], null);
		}else{
			runGraph.initEngine(null, null);
		}

		System.out.println("Graph definition file: "+args[0]);
		System.out.println("Plugins directory: "+ args[1] != null ? args[1] : Defaults.DEFAULT_PLUGINS_DIRECTORY);
		
		try{
			in=new FileInputStream(args[0]);
		}
		catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}
		
		
		TransformationGraph graph= new TransformationGraph();
		TransformationGraphXMLReaderWriter graphReader=new TransformationGraphXMLReaderWriter(graph);
		
		try{
			graphReader.read(in);
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		if(!graph.init()){
		System.err.println("Graph initialization failed !");
		return;
		}
		
		graph.dumpGraphConfiguration();
		
		if (!graph.run().equals(Result.FINISHED_OK)){ // start all Nodes (each node is one thread)
		System.out.println("Failed starting all nodes!");
		return;		
		}
	
	}











}

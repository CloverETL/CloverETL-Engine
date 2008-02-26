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
import java.util.concurrent.Future;

import org.jetel.data.Defaults;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphExecutor;
import org.jetel.graph.runtime.GraphRuntimeContext;

public class testXMLGraph{

	public static void main(String args[]){
		
		if (args.length<1){
			System.out.println("Usage: testXMLGraph <graph file name> <plug-ins directory> <default properties file>");
		}
		
		FileInputStream in;
        
        //initialization; must be present
		EngineInitializer.initEngine(args.length > 1 ? args[1] : null, args.length > 2 ? args[2] : null, null);

		System.out.println("Graph definition file: "+args[0]);
		System.out.println("Plugins directory: "+ (args.length > 1 ? args[1] : Defaults.DEFAULT_PLUGINS_DIRECTORY));
		
		try{
			in=new FileInputStream(args[0]);
		}
		catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}
		
		
		TransformationGraph graph= new TransformationGraph();
		TransformationGraphXMLReaderWriter graphReader=new TransformationGraphXMLReaderWriter(null);
		
		try{
			graph = graphReader.read(in);
		}catch(Exception ex){
			ex.printStackTrace();
		}
				
		try{
			graph.init();
		}catch(Exception ex){
			ex.printStackTrace();
		}

		graph.dumpGraphConfiguration();
		
		//prepare runtime parameters - JMX is turned off
	    GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
	    runtimeContext.setUseJMX(false);
	    
		GraphExecutor executor = new GraphExecutor();
		
		Future<Result> result;
		try{
			result = executor.runGraph(graph, runtimeContext);
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

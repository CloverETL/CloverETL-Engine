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

import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.TransformationGraphXMLReaderWriter;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.graph.runtime.GraphRuntimeContext;
import org.jetel.main.runGraph;

public class testXMLGraph{

	private final static String PARAMETER_FILE = "params.txt"; 
	private final static String PLUGINS_PROPERTY = "plugins";

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

		if (args.length<1){
			System.out.println("Usage: testXMLGraph <graph file name> [<plug-ins directory> <default properties file>]");
			System.exit(1);
		}
		
		String plugins = null;
		if (args.length > 1){
			plugins = args[1];
		}else {
			plugins = arguments.getProperty(PLUGINS_PROPERTY);
		}
		
		String propertiesFile = null;
		if (args.length > 2) {
			propertiesFile = args[2];
		}
			
		FileInputStream in;
        
        //initialization; must be present
		EngineInitializer.initEngine(plugins, propertiesFile, null);

		System.out.println("Graph definition file: "+args[0]);
		System.out.println("Plugins directory: "+ plugins);
		System.out.println("Default properties file: "+ propertiesFile);
		
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

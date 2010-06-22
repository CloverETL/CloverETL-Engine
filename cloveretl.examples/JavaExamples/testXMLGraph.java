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

/**
 * Usage: testXMLGraph [-plugins pluginsDirectory] [-config propertiesFile] graphFile
 * 
 * This example illustrates how run from java code transformation graph stored in xml file.
 * All examples require to provide CloverETL plugins. Plugins directory can be set as program argument or 
 * in params.txt file as plugins parameter or required plugins have to be set on classpath when running the program. 
 * When set in params.txt, the same directory is used for all examples and musn't be set 
 * for each example separately.
 *
 */
public class testXMLGraph{

	private final static String PARAMETER_FILE = "params.txt"; 
	private final static String PLUGINS_PROPERTY = "plugins";
	private final static String PROPERTIES_FILE_PROPERTY = "config";

	public static void main(String args[]){
		
		String plugins = null;
		String propertiesFile = null;
		
		//read parameters from parameter file
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
		plugins = arguments.getProperty(PLUGINS_PROPERTY);
		propertiesFile = arguments.getProperty(PROPERTIES_FILE_PROPERTY);
		//override parameters with program arguments
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("-" + PLUGINS_PROPERTY)) {
				plugins = args[++i];
			}else if (args[i].startsWith("-" + PROPERTIES_FILE_PROPERTY)) {
				propertiesFile = args[++i];
			}
		}

		if (args.length<1){
			System.out.println("Usage: testXMLGraph [-plugins pluginsDirectory] [-config propertiesFile] graphFile");
			System.exit(1);
		}
					
		String graphFile = args[args.length - 1];
		
		System.out.println("**************** Input parameters: ****************");
		System.out.println("Graph definition file: "+graphFile);
		System.out.println("Plugins directory: "+ plugins);
		System.out.println("Default properties file: "+ propertiesFile);
		System.out.println("***************************************************");

		FileInputStream in;
        
        //initialization; must be present
		EngineInitializer.initEngine(plugins, propertiesFile, null);

		//prepare runtime parameters - JMX is turned off
	    GraphRuntimeContext runtimeContext = new GraphRuntimeContext();
	    runtimeContext.setUseJMX(false);
		
		//create transformation graph from xml file
		TransformationGraph graph= new TransformationGraph();
		TransformationGraphXMLReaderWriter graphReader=new TransformationGraphXMLReaderWriter(runtimeContext);
		
		try{
			in=new FileInputStream(graphFile);
		}
		catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}
		
		try{
			graph = graphReader.read(in);
		}catch(Exception ex){
			ex.printStackTrace();
		}
				
		graph.dumpGraphConfiguration();
		
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

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

import org.jetel.component.ComponentFactory;
import org.jetel.data.Defaults;
import org.jetel.graph.*;

public class testXMLGraph{

	public static void main(String args[]){
		FileInputStream in;
        
        //initialization; must be present
        Defaults.init();
        ComponentFactory.init();

		System.out.println("Graph definition file: "+args[0]);
		
		try{
			in=new FileInputStream(args[0]);
		}
		catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}
		
		
		TransformationGraphXMLReaderWriter graphReader=TransformationGraphXMLReaderWriter.getReference();
		TransformationGraph graph= TransformationGraph.getReference();
		
		if (!graphReader.read(graph,in)){
			System.err.println("Error in reading graph from XML!");
			return;
		}
		
		if(!graph.init(null)){
		System.err.println("Graph initialization failed !");
		return;
		}
		
		graph.dumpGraphConfiguration();
		
		if (!graph.run()){ // start all Nodes (each node is one thread)
		System.out.println("Failed starting all nodes!");
		return;		
		}
	
	}











}

/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.component;

//import org.w3c.dom.Node;
import java.util.Map;
import java.util.HashMap;
import java.lang.reflect.Method;
import org.jetel.graph.Node;

/**
 *  Description of the Class
 *
 * @author     dpavlis
 * @since    May 27, 2002
 * @revision $Revision$
 */
public class ComponentFactory {

	private final static String NAME_OF_STATIC_LOAD_FROM_XML = "fromXML";
	private final static Class[] PARAMETERS_FOR_METHOD = new Class[] { org.w3c.dom.Node.class };
	private final static Map componentMap = new HashMap();
	
	static{
		// register known components
		// parameters <component type>,<full class name including package>
		registerComponent(SimpleCopy.COMPONENT_TYPE,"org.jetel.component.SimpleCopy");
		registerComponent(Concatenate.COMPONENT_TYPE,"org.jetel.component.Concatenate");
		registerComponent(DelimitedDataReader.COMPONENT_TYPE,"org.jetel.component.DelimitedDataReader");
		registerComponent(DelimitedDataWriter.COMPONENT_TYPE,"org.jetel.component.DelimitedDataWriter");
		registerComponent(SimpleGather.COMPONENT_TYPE,"org.jetel.component.SimpleGather");
		registerComponent(DelimitedDataWriterNIO.COMPONENT_TYPE,"org.jetel.component.DelimitedDataWriterNIO");
		registerComponent(DelimitedDataReaderNIO.COMPONENT_TYPE,"org.jetel.component.DelimitedDataReaderNIO");
		registerComponent(Reformat.COMPONENT_TYPE,"org.jetel.component.Reformat");
		registerComponent(DBInputTable.COMPONENT_TYPE,"org.jetel.component.DBInputTable");
		registerComponent(Sort.COMPONENT_TYPE,"org.jetel.component.Sort");
		registerComponent(DBOutputTable.COMPONENT_TYPE,"org.jetel.component.DBOutputTable");
		registerComponent(FixLenDataWriterNIO.COMPONENT_TYPE,"org.jetel.component.FixLenDataWriterNIO");
		registerComponent(Dedup.COMPONENT_TYPE,"org.jetel.component.Dedup");
		registerComponent(FixLenDataReaderNIO.COMPONENT_TYPE,"org.jetel.component.FixLenDataReaderNIO");
		registerComponent("FIXED_DATA_READER_NIO","org.jetel.component.FixLenDataReaderNIO");
		registerComponent(Merge.COMPONENT_TYPE,"org.jetel.component.Merge");
		registerComponent(MergeJoin.COMPONENT_TYPE,"org.jetel.component.MergeJoin");
		registerComponent("SORTED_JOIN","org.jetel.component.MergeJoin"); // synonym for MergeJoin (former name)
		registerComponent(Trash.COMPONENT_TYPE,"org.jetel.component.Trash"); 
		registerComponent(Filter.COMPONENT_TYPE,"org.jetel.component.Filter");
		registerComponent(DBExecute.COMPONENT_TYPE,"org.jetel.component.DBExecute");
		registerComponent(HashJoin.COMPONENT_TYPE,"org.jetel.component.HashJoin");
		registerComponent(CheckForeignKey.COMPONENT_TYPE,"org.jetel.component.CheckForeignKey");
		registerComponent(DBFDataReader.COMPONENT_TYPE,"org.jetel.component.DBFDataReader");
	}
	
	
	public final static void registerComponent(Node component){
		componentMap.put(component.getType(),component.getName());
	}
	
	public final static void registerComponent(String componentType,String className){
		componentMap.put(componentType,className);
	}
	
	/**
	 *  Method for creating various types of Components based on component type & XML parameter definition.<br>
	 *  If component type is not registered, it tries to use componentType parameter directly as a class name.
	 *  This way new components can be added withou modifying ComponentFactory code.
	 *  
	 * @param  componentType  Type of the component (e.g. SimpleCopy, Gather, Join ...)
	 * @param  xmlNode        XML element containing appropriate Node parameters
	 * @return                requested Component (Node) object or null if creation failed 
	 * @since                 May 27, 2002
	 */
	public final static Node createComponent(String componentType, org.w3c.dom.Node nodeXML) {
		
			// try to load the component (use type as a full name)
			Class tClass;
			Method method;
			String className=null;
			try{
				className=(String)componentMap.get(componentType);
				if (className==null){
					// if it cannot be translated into class name, try to use the component
					// type as fully qualified component name
					className=componentType;
				}
				tClass = Class.forName(className);
			}catch(ClassNotFoundException ex){
				throw new RuntimeException("Unknown component: " + componentType + " class: "+className);
			}catch(Exception ex){
				throw new RuntimeException("Unknown exception: " + ex);
			}
			try{
				Object[] args={nodeXML};
				//Class.forName("org.w3c.dom.Node")
				method=tClass.getMethod(NAME_OF_STATIC_LOAD_FROM_XML, PARAMETERS_FOR_METHOD);
				//return newNode.fromXML(nodeXML);
				return (org.jetel.graph.Node)method.invoke(null,args);
			}catch(Exception ex){
				throw new RuntimeException("Can't create object of : " + componentType + " exception: "+ex);
			}
		
	}
}



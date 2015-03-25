/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.enums;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.SubgraphPort;
import org.jetel.graph.SubgraphPorts;
import org.jetel.graph.TransformationGraph;


/**
 * This enumeration represents all possible values of 'enable' attribute of components.
 * The EnabledEnum is no more regular enum. Dynamic instances related with subgraph ports
 * were added to existing static instances like ENABLED, DISABLED...
 * 
 * The core method is {@link #isEnabled()}, which indicates the 'enability' of related component(s).

 * To enabled a component use one of instances ENABLED and TRUE, and to disable a component
 * use one of the instances DISABLED, PASS_THROUGH and FALSE.
 * 
 * For conditional enabling use following pattern ENABLE_WHEN_(INPUT_PORT|OUTPUT_PORT)_(\\d?)_IS_(CONNECTED|DISCONNECTED),
 * for example: ENABLE_WHEN_INPUT_PORT_0_IS_CONNECTED, ENABLE_WHEN_OUTPUT_PORT_1_IS_DISCONNECTED 
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 5.4.2007
 */
public class EnabledEnum {
   
    public static final EnabledEnum ENABLED = new EnabledEnum("enabled", true);
    public static final EnabledEnum DISABLED = new EnabledEnum("disabled", false);
    public static final EnabledEnum PASS_THROUGH = new EnabledEnum("passThrough", false); //deprecated - use DISABLED or FALSE
    public static final EnabledEnum TRUE = new EnabledEnum("true", true);
    public static final EnabledEnum FALSE = new EnabledEnum("false", false);
    
    private static final EnabledEnum[] values = new EnabledEnum[] { ENABLED, DISABLED, PASS_THROUGH, TRUE, FALSE }; 
    
    private String id;
    
    private boolean enabled;

	private EnabledEnum(String id) {
		this.id = id;
	}
    
    private EnabledEnum(String id, boolean enabled) {
        this.id = id;
        this.enabled = enabled;
    }
    
    public static EnabledEnum fromString(String id) {
        return fromString(id, null);
    }
    
    public static EnabledEnum fromString(String id, EnabledEnum defaultValue) {
        if (id == null) {
        	return defaultValue;
        }
        
        //try static values
        for (EnabledEnum item : values) {
            if (id.equalsIgnoreCase(item.id)) {
                return item;
            }
        }
        
        //try dynamic values
        EnabledEnum result = SubgraphPortsDynamicValues.fromString(id);
        
        return result != null ? result : defaultValue;
    }
    
	/**
     * @return true if a component with this value is kept enabled, false 
     *  if a component with this value is removed from graph
     */
    public boolean isEnabled() {
    	return enabled;
    }
    
    /**
     * @return true for dynamic enable values (for example ENABLE_WHEN_INPUT_PORT_0_IS_CONNECTED)
     */
    public boolean isDynamic() {
    	return false;
    }
    
    @Override
	public String toString() {
        return id;
    }
 
    /**
     * This class handles dynamic 'enable' constants derived from subgraph ports.
     */
    public static class SubgraphPortsDynamicValues {
    	private static final String PATTERN_STRING = "ENABLE_WHEN_(INPUT_PORT|OUTPUT_PORT)_(\\d?)_IS_(CONNECTED|DISCONNECTED)";
    	
    	private static final Pattern PATTERN = Pattern.compile(PATTERN_STRING);
    	
		/**
		 * @param id
		 * @return enable constant for given identifier
		 */
		private static EnabledEnum fromString(String id) {
			Matcher matcher = PATTERN.matcher(id);
			if (matcher.matches()) {
				boolean inputPort = matcher.group(1).equals("INPUT_PORT");
				int portIndex = Integer.parseInt(matcher.group(2));
				boolean connected = matcher.group(3).equals("CONNECTED");
				return new SubgraphPortsEnabledEnum(id, inputPort, portIndex, connected);
			} else {
				return null;
			}
		}
		
		/**
		 * @param inputPort true for input subgraph port
		 * @param portIndex index of port
		 * @param connected true for connected subgraph port
		 * @return name of dynamic 'enable' constant based on subgraph ports 
		 */
		public static String getName(boolean inputPort, int portIndex, boolean connected) {
			return "ENABLE_WHEN_" + (inputPort ? "INPUT_PORT" : "OUTPUT_PORT") + "_" + portIndex + "_IS_" + (connected ? "CONNECTED" : "DISCONNECTED");
		}
		
    	/**
    	 * EnabledEnum which represents dynamic 'enable' constant base on subgraph ports.
    	 */
    	private static class SubgraphPortsEnabledEnum extends EnabledEnum {
    		private boolean inputPort;
    		private int portIndex;
    		private boolean connected;
    		
			public SubgraphPortsEnabledEnum(String id, boolean inputPort, int portIndex, boolean connected) {
				super(id);
				this.inputPort = inputPort;
				this.portIndex = portIndex;
				this.connected = connected;
			}
    		
    		@Override
    		public boolean isEnabled() {
    			TransformationGraph graph = ContextProvider.getGraph();
    			if (graph != null) {
    				SubgraphPorts subgraphPorts = inputPort ? graph.getSubgraphInputPorts() : graph.getSubgraphOutputPorts();
    				if (subgraphPorts.hasPort(portIndex)) {
    					SubgraphPort port = subgraphPorts.getPorts().get(portIndex);
    					return connected ? port.isConnected() : !port.isConnected();
    				} else {
    					throw new JetelRuntimeException("Component enable attribute cannot be resolved, subgraph port " + portIndex + " does not exist.");
    				}
    			} else {
    				throw new JetelRuntimeException("Component enable attribute cannot be resolved, no graph available.");
    			}
    		}
    		
    		@Override
    		public boolean isDynamic() {
    			return true;
    		}
    	}
    }
    
}

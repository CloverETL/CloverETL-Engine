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

import org.jetel.graph.ContextProvider;
import org.jetel.graph.SubgraphPort;
import org.jetel.graph.SubgraphPorts;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.WatchDog;


/**
 * This enumeration represents all possible values of 'enable' attribute of components.
 * The EnabledEnum is no more regular enum. Dynamic instances related with subgraph ports
 * were added to existing static instances like ENABLED, DISABLED...
 * 
 * The core method is {@link #isEnabled()}, which indicates the 'enability' of related component(s).

 * To enabled a component use one of instances ALWAYS, ENABLED and TRUE, and to disable a component
 * use one of the instances NEVER, DISABLED, PASS_THROUGH and FALSE.
 * 
 * For conditional enabling use following pattern enableWhen(InputPort|OutputPort)_(\\d?)Is(Connected|Disconnected),
 * for example: enableWhenInputPort0IsConnected, enableWhenOutputPort1IsDisconnected 
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 5.4.2007
 */
public class EnabledEnum {
   
    public static final EnabledEnum ALWAYS = new EnabledEnum("always", "Always", true);
    public static final EnabledEnum NEVER = new EnabledEnum("never", "Never", false);
    public static final EnabledEnum ENABLED = new EnabledEnum("enabled", "Enabled", true);
    public static final EnabledEnum DISABLED = new EnabledEnum("disabled", "Disabled", false);
    public static final EnabledEnum PASS_THROUGH = new EnabledEnum("passThrough", "Pass through", false); //deprecated - use DISABLED or FALSE
    public static final EnabledEnum TRUE = new EnabledEnum("true", "Enabled", true);
    public static final EnabledEnum FALSE = new EnabledEnum("false", "Disabled", false);
    
    //CLO-397 - this is the "cascade disable" option
    //component will remain in graph and all subsequent "non-trashed" components will be considered as disabled
    public static final EnabledEnum TRASH = new EnabledEnum("trash", "Trash", true);
    
    //discard is non-public enabled status which is used by clustered graphs to remove components from graph with all related edges
    //regular disabled component is replaced by a 'pass-through' edge if possible, discarded component wipes out all related edges
    //see TransformationGraphAnalyser#disableNodesInPhases()
    public static final EnabledEnum DISCARD = new EnabledEnum("discard", "Discard", false);
    
    private static final EnabledEnum[] values = new EnabledEnum[] { ALWAYS, NEVER, ENABLED, DISABLED, PASS_THROUGH, TRUE, FALSE, DISCARD, TRASH }; 

	public static final EnabledEnum DEFAULT_VALUE = ENABLED;

    private String id;
    
    private String label;
    
    private boolean enabled;

	private EnabledEnum(String id) {
		this(id, null, false);
	}
    
    private EnabledEnum(String id, String label, boolean enabled) {
        this.id = id;
        this.label = label;
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
        
        return result != null ? result : new UnknownEnabledEnum(id);
    }
    
    /**
     * @return human readable specification of enabled value, which is used mainly in designer widgets
     */
    public String getLabel() {
    	return label != null ? label : toString();
    }
    
	/**
	 * This is very similar to {@link #getLabel()} with slightly different label composition.
	 * @return human readable status of enabled value, which is used by engine logging
	 * @see WatchDog#printComponentsEnabledStatus()
	 */
	public String getStatus() {
		return getLabel();
	}

	/**
     * @return true if a component with this value is kept enabled, false 
     *  if a component with this value is removed from graph
     */
    public boolean isEnabled() {
    	return enabled;
    }
    
    /**
     * @return true if a component with this value is kept in graph but "replaced" with trash
     */
    public boolean isBlocker() {
    	return this == TRASH;
    }
    
    /**
     * @return null for valid {@link EnabledEnum} instance or error message otherwise
     */
    public String validate() {
    	return null;
    }
    
    /**
     * @return true for dynamic enable values (for example enableWhenInputPort0IsConnected)
     */
    public boolean isDynamic() {
    	return false;
    }
    
    @Override
	public String toString() {
        return id;
    }
 
    /**
     * This class represents unknown type of {@link EnabledEnum}.
     * It considered as invalid value, see {@link #validate()} method.
     */
    public static class UnknownEnabledEnum extends EnabledEnum {
		public UnknownEnabledEnum(String id) {
			super(id);
		}
		
		@Override
		public boolean isEnabled() {
			return DEFAULT_VALUE.isEnabled();
		}
		
		@Override
		public String validate() {
			return "Invalid 'enabled' attribute: '" + this + "'.";
		}
    }
    
    /**
     * This class handles dynamic 'enable' constants derived from subgraph ports.
     */
    public static class SubgraphPortsDynamicValues {
    	private static final String PATTERN_STRING = "enableWhen(InputPort|OutputPort)(\\d+)Is(Connected|Disconnected)";
    	
    	private static final Pattern PATTERN = Pattern.compile(PATTERN_STRING);
    	
		/**
		 * @param id
		 * @return enable constant for given identifier
		 */
		private static EnabledEnum fromString(String id) {
			Matcher matcher = PATTERN.matcher(id);
			if (matcher.matches()) {
				boolean inputPort = matcher.group(1).equals("InputPort");
				int portIndex = Integer.parseInt(matcher.group(2));
				boolean connected = matcher.group(3).equals("Connected");
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
		public static String getId(boolean inputPort, int portIndex, boolean connected) {
			return "enableWhen" + (inputPort ? "InputPort" : "OutputPort") + portIndex + "Is" + (connected ? "Connected" : "Disconnected");
		}

		public static String getLabel(boolean inputPort, int portIndex, boolean connected) {
			return "Enable when " + (inputPort ? "input port" : "output port") + " " + portIndex + " is " + (connected ? "connected" : "disconnected");
		}

		public static String getStatus(boolean inputPort, int portIndex, boolean connected) {
			return (inputPort ? "Input port" : "Output port") + " " + portIndex + " is " + (connected ? "connected" : "disconnected");
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
			public String getLabel() {
				return SubgraphPortsDynamicValues.getLabel(inputPort, portIndex, connected);
			}
			
			@Override
			public String getStatus() {
				return SubgraphPortsDynamicValues.getStatus(inputPort, portIndex, isEnabled() ? connected : !connected);
			}
			
    		@Override
    		public boolean isEnabled() {
    			if (validate() == null) {
        			TransformationGraph graph = ContextProvider.getGraph();
    				SubgraphPorts subgraphPorts = inputPort ? graph.getSubgraphInputPorts() : graph.getSubgraphOutputPorts();
					SubgraphPort port = subgraphPorts.getPorts().get(portIndex);
					return connected ? port.isConnected() : !port.isConnected();
    			} else {
    				return true;
    			}
    		}
    		
    		@Override
    		public boolean isDynamic() {
    			return true;
    		}
    		
    		@Override
    		public String validate() {
    			TransformationGraph graph = ContextProvider.getGraph();
    			if (graph != null) {
    				SubgraphPorts subgraphPorts = inputPort ? graph.getSubgraphInputPorts() : graph.getSubgraphOutputPorts();
    				if (subgraphPorts.hasPort(portIndex)) {
    					SubgraphPort port = subgraphPorts.getPorts().get(portIndex);
    					if (!port.isRequired()) {
    						return null;
    					} else {
        					return "'Enabled' attribute cannot be resolved, subgraph " + (inputPort ? "input" : "output") + " port " + portIndex + " is not optional.";
    					}
    				} else {
    					return "'Enabled' attribute cannot be resolved, subgraph " + (inputPort ? "input" : "output") + " port " + portIndex + " does not exist.";
    				}
    			} else {
    				return "'Enabled' attribute cannot be resolved, no graph available.";
    			}
    		}
    	}
    }
    
}

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
package org.jetel.ctl.extensions;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jetel.ctl.Stack;
import org.jetel.ctl.data.TLTypeEnum;
import org.jetel.data.DataRecord;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.GraphParameter;
import org.jetel.graph.GraphParameters;
import org.jetel.graph.Node;
import org.jetel.graph.SubgraphPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.HashCodeUtil;
import org.jetel.util.property.PropertiesUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

public class UtilLib extends TLFunctionLibrary {

    @Override
    public TLFunctionPrototype getExecutable(String functionName) {
		if (functionName != null) {
			switch (functionName) {
				case "sleep": return new SleepFunction();
				case "randomUUID": return new RandomUuidFunction();
				case "getParamValue": return new GetParamValueFunction();
				case "getParamValues": return new GetParamValuesFunction();
				case "getRawParamValue": return new GetRawParamValueFunction();
				case "getRawParamValues": return new GetRawParamValuesFunction();
				case "getJavaProperties": return new GetJavaPropertiesFunction();
				case "getEnvironmentVariables": return new GetEnvironmentVariablesFunction();
				case "getComponentProperty": return new GetComponentPropertyFunction();
				case "hashCode": return new HashCodeFunction();
				case "byteAt": return new ByteAtFunction();
				case "getSubgraphInputPortsCount": return new GetSubgraphInputPortsCountFunction();
				case "getSubgraphOutputPortsCount": return new GetSubgraphOutputPortsCountFunction();
				case "isSubgraphInputPortConnected": return new IsSubgraphInputPortConnectedFunction();
				case "isSubgraphOutputPortConnected": return new IsSubgraphOutputPortConnectedFunction();
				case "parseProperties": return new ParsePropertiesFunction(); //$NON-NLS-1$
//	    		case "byteSet": return new ByteSetFunction();
			}
		} 
    		
		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    }
    
	private static String LIBRARY_NAME = "Util";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}


    // SLEEP
    @TLFunctionAnnotation("Pauses execution for specified milliseconds")
    public static final void sleep(TLFunctionCallContext context, long millis) {
    	try {
    		Thread.sleep(millis);
    	} catch (InterruptedException e) {
    		throw new JetelRuntimeException(e);
		}
    }

    static class SleepFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			long millis = stack.popLong();
			sleep(context, millis);
		}
    	
    }
    
    // UUID
    @TLFunctionAnnotation("Generates random universally unique identifier (UUID)")
    public static String randomUUID(TLFunctionCallContext context) {
    	return UUID.randomUUID().toString();
    }
    
    static class RandomUuidFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(randomUUID(context));
    	}
    }
    
    // GET PARAM VALUE
	@TLFunctionAnnotation("Returns the resolved value of a graph parameter")
    public static String getParamValue(TLFunctionCallContext context, String paramName) {
		if (!StringUtils.isEmpty(paramName)) {
			return ((TLPropertyRefResolverCache) context.getCache()).getCachedPropertyRefResolver().getResolvedPropertyValue(paramName, RefResFlag.SPEC_CHARACTERS_OFF);
		} else {
			return null;
		}
    }
    
    @TLFunctionInitAnnotation()
    public static final void getParamValueInit(TLFunctionCallContext context) {
		PropertyRefResolver refResolver
			= context.getGraph() != null ? context.getGraph().getPropertyRefResolver() : new PropertyRefResolver();
		
		context.setCache(new TLPropertyRefResolverCache(refResolver));
    }

    static class GetParamValueFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    		getParamValueInit(context);
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
			String paramName = stack.popString();
    		stack.push(getParamValue(context, paramName));
    	}
    }

    // GET RAW PARAM VALUE
	@TLFunctionAnnotation("Returns the unresolved value of a graph parameter")
    public static String getRawParamValue(TLFunctionCallContext context, String paramName) {
		if (!StringUtils.isEmpty(paramName)) {
			TransformationGraph graph = context.getGraph();
			if (graph != null) {
				return graph.getGraphParameters().getGraphParameter(paramName).getValue();
			}
		}
		return null;
    }
    
	static class GetRawParamValueFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
			String paramName = stack.popString();
    		stack.push(getRawParamValue(context, paramName));
    	}
    }

    // GET PARAM VALUES
    @SuppressWarnings("unchecked")
	@TLFunctionAnnotation("Returns an unmodifiable map of resolved values of graph parameters")
    public static Map<String, String> getParamValues(TLFunctionCallContext context) {
		return ((TLObjectCache<Map<String, String>>) context.getCache()).getObject();
    }
    
    @TLFunctionInitAnnotation()
    public static final void getParamValuesInit(TLFunctionCallContext context) {
    	PropertyRefResolver refResolver
    		= context.getGraph() != null ? context.getGraph().getPropertyRefResolver() : new PropertyRefResolver();
    	GraphParameters parameters = refResolver.getGraphParameters();
		
		Map<String, String> map = new HashMap<String, String>();
		//built-in graph parameters are taken first to be potentially overridden by user's graph parameters
		for (GraphParameter param : parameters.getAllBuiltInParameters()) {
			map.put(param.getName(), refResolver.getResolvedPropertyValue(param.getName(), RefResFlag.SPEC_CHARACTERS_OFF));
		}
		for (GraphParameter param : parameters.getAllGraphParameters()) {
			map.put(param.getName(), refResolver.getResolvedPropertyValue(param.getName(), RefResFlag.SPEC_CHARACTERS_OFF));
		}
		context.setCache(new TLObjectCache<Map<String, String>>(Collections.unmodifiableMap(map)));
    }

    static class GetParamValuesFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    		getParamValuesInit(context);
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(getParamValues(context));
    	}
    }

    // GET RAW PARAM VALUES
    @SuppressWarnings("unchecked")
	@TLFunctionAnnotation("Returns an unmodifiable map of unresolved values of graph parameters")
    public static Map<String, String> getRawParamValues(TLFunctionCallContext context) {
		return ((TLObjectCache<Map<String, String>>) context.getCache()).getObject();
    }
    
    @TLFunctionInitAnnotation()
    public static final void getRawParamValuesInit(TLFunctionCallContext context) {
    	TransformationGraph graph = context.getGraph();
		
		Map<String, String> map = new HashMap<String, String>();
		if (graph != null) {
			for (GraphParameter param : graph.getGraphParameters().getAllGraphParameters()) {
				map.put(param.getName(), param.getValue());
			}
		}
		context.setCache(new TLObjectCache<Map<String, String>>(Collections.unmodifiableMap(map)));
    }

    static class GetRawParamValuesFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    		getRawParamValuesInit(context);
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(getRawParamValues(context));
    	}
    }

    // GET JAVA PROPERTIES
    /*
     * Uses unchecked conversion - if someone obtains
     * System.getProperties().put(1, 2),
     * the map will be broken and is likely to throw class cast exceptions.
     */
    @SuppressWarnings("unchecked")
	@TLFunctionAnnotation("Returns a map of Java VM properties")
    public static Map<String, String> getJavaProperties(TLFunctionCallContext context) {
    	Map<?, ?> map = (Map<Object, Object>) System.getProperties();
		return (Map<String, String>) map;
    }
    
    static class GetJavaPropertiesFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(getJavaProperties(context));
    	}
    }

    // GET ENVIRONMENT VARIABLES
    @TLFunctionAnnotation("Returns a map of environment variables. The map is unmodifiable.")
    public static Map<String, String> getEnvironmentVariables(TLFunctionCallContext context) {
		return System.getenv();
    }
    
    static class GetEnvironmentVariablesFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(getEnvironmentVariables(context));
    	}
    }
    
    // GET COMPONENT PROPERTY
    @TLFunctionAnnotation("Returns the value of a component property.")
    public static String getComponentProperty(TLFunctionCallContext context, String name) {
    	Node node = context.getTransformationContext().getNode();
    	if (node == null || node.getAttributes() == null) {
    		throw new IllegalStateException("Component properties are not available");
    	}
    	if (name == null) {
    		return null;
    	}
		return node.getAttributes().getProperty(name);
    }
    
    static class GetComponentPropertyFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		String name = stack.popString();
    		stack.push(getComponentProperty(context, name));
    	}
    }
    
    // HASH CODE
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final int hashCode(TLFunctionCallContext context, int i) {
 		return HashCodeUtil.hash(i);
 	}
 	
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final int hashCode(TLFunctionCallContext context, long i) {
 		return HashCodeUtil.hash(i);
 	}
 	
 	
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final int hashCode(TLFunctionCallContext context, double i) {
 		return HashCodeUtil.hash(i);
 	}
 	
    
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
   	public static final int hashCode(TLFunctionCallContext context, boolean i) {
   		return HashCodeUtil.hash(i);
   	}
 	
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final int hashCode(TLFunctionCallContext context, BigDecimal i) {
 		return HashCodeUtil.hash(i);
 	}
 	
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final int hashCode(TLFunctionCallContext context, String i) {
    	return HashCodeUtil.hash(i);
 	}
    
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final int hashCode(TLFunctionCallContext context, java.util.Date i) {
 		return HashCodeUtil.hash(i);
 	}
    
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final <E> int hashCode(TLFunctionCallContext context, List<E> list) {
 		return HashCodeUtil.hash(list);
 	}
 	
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final <K,V> int hashCode(TLFunctionCallContext context, Map<K,V> map) {
 		return HashCodeUtil.hash(map);
 	}
 	
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final int hashCode(TLFunctionCallContext context, byte[] i) {
 		return HashCodeUtil.hash(i);
 	}
 	
    @TLFunctionAnnotation("Returns parameter's hashCode - i.e. Java's hashCode().")
 	public static final int hashCode(TLFunctionCallContext context, DataRecord rec) {
 		return rec.hashCode();
 	}
    
      
    static class HashCodeFunction implements TLFunctionPrototype {

    	private class ParamCache extends TLCache{
    		TLTypeEnum type;
    		ParamCache(TLTypeEnum type){
    			this.type=type;
    		}
    		TLTypeEnum getType(){
    			return type;
    		}
    	}
    	
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
			context.setCache(new ParamCache(TLTypeEnum.convertParamType(context.getParams()[0])));
		}
    	
		@Override
		// TODO: implementation not consistent with compiled version of function
		// due to missing switchable property/attrib of TLType..
		public void execute(Stack stack, TLFunctionCallContext context) {
			switch (((ParamCache) context.getCache()).getType()) {
			case STRING:
				stack.push(HashCodeUtil.hash(stack.popString()));
				break;
			case INT:
				stack.push(HashCodeUtil.hash(stack.popInt().intValue()));
				break;
			case LONG:
				stack.push(HashCodeUtil.hash(stack.popLong().longValue()));
				break;
			case DECIMAL:
				stack.push(HashCodeUtil.hash(stack.popDecimal()));
				break;
			case DOUBLE:
				stack.push(HashCodeUtil.hash(stack.popDouble().doubleValue()));
				break;
			case BYTEARRAY:
				stack.push(HashCodeUtil.hash(stack.popByteArray()));
				break;
			case DATE:
				stack.push(HashCodeUtil.hash(stack.popDate()));
				break;
			case BOOLEAN:
				stack.push(HashCodeUtil.hash(stack.popBoolean().booleanValue()));
				break;
			case MAP:
				stack.push(HashCodeUtil.hash(stack.popMap()));
				break;
			case LIST:
				stack.push(HashCodeUtil.hash(stack.popList()));
				break;
			case RECORD:
				stack.push(stack.pop().hashCode());
				break;
			default:
				stack.push(stack.pop().hashCode());
			}
		}

    }
    
    // BYTE AT
 	@TLFunctionAnnotation("Returns byte at the specified position of input bytearray")
 	public static final Integer byteAt(TLFunctionCallContext context, byte[] input, Integer position) {
 		return Integer.valueOf(0xff & input[position]);
 	}

 	static class ByteAtFunction implements TLFunctionPrototype {

 		@Override
 		public void init(TLFunctionCallContext context) {
 		}

 		@Override
 		public void execute(Stack stack, TLFunctionCallContext context) {
 			final Integer pos = stack.popInt();
 			final byte[] input = stack.popByteArray();
 			stack.push(byteAt(context, input, pos));
 		}
 	}
 	
 	/*
 	// BYTE SET
  	@TLFunctionAnnotation("Sets the byte at the specified position of input bytearray")
  	public static final void byteSet(TLFunctionCallContext context, byte[] input, int position, int value) {
  		input[position]= (byte)( value & 0xff);
  	}

  	class ByteSetFunction implements TLFunctionPrototype {

  		@Override
  		public void init(TLFunctionCallContext context) {
  		}

  		@Override
  		public void execute(Stack stack, TLFunctionCallContext context) {
  			final int value = stack.popInt();
  			final int pos = stack.popInt();
  			final byte[] input = stack.popByteArray();
  			byteSet(context, input, pos, value);
  		}
  	}
  	*/

    // GET SUBGRAPH INPUT PORTS COUNT
	@SuppressWarnings("unchecked")
	@TLFunctionAnnotation("Returns number of input ports for this subgraph")
    public static Integer getSubgraphInputPortsCount(TLFunctionCallContext context) {
		return ((TLObjectCache<Integer>) context.getCache()).getObject();
    }
    
    @TLFunctionInitAnnotation()
    public static final void getSubgraphInputPortsCountInit(TLFunctionCallContext context) {
    	if (context.getGraph().getStaticJobType().isSubJob()) {
    		context.setCache(new TLObjectCache<Integer>(context.getGraph().getSubgraphInputPorts().getPorts().size()));
    	} else {
    		context.setCache(new TLObjectCache<Integer>(null));
    	}
    }

    static class GetSubgraphInputPortsCountFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    		getSubgraphInputPortsCountInit(context);
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(getSubgraphInputPortsCount(context));
    	}
    }

    // GET SUBGRAPH OUTPUT PORTS COUNT
	@SuppressWarnings("unchecked")
	@TLFunctionAnnotation("Returns number of output ports for this subgraph")
    public static Integer getSubgraphOutputPortsCount(TLFunctionCallContext context) {
		return ((TLObjectCache<Integer>) context.getCache()).getObject();
    }
    
    @TLFunctionInitAnnotation()
    public static final void getSubgraphOutputPortsCountInit(TLFunctionCallContext context) {
    	if (context.getGraph().getStaticJobType().isSubJob()) {
    		context.setCache(new TLObjectCache<Integer>(context.getGraph().getSubgraphOutputPorts().getPorts().size()));
    	} else {
    		context.setCache(new TLObjectCache<Integer>(null));
    	}
    }

    static class GetSubgraphOutputPortsCountFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    		getSubgraphOutputPortsCountInit(context);
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(getSubgraphOutputPortsCount(context));
    	}
    }

    // IS SUBGRAPH INPUT PORT CONNECTED
	@TLFunctionAnnotation("Returns true if specified input port is connected")
    public static boolean isSubgraphInputPortConnected(TLFunctionCallContext context, int portIndex) {
    	if (context.getGraph().getStaticJobType().isSubJob()) {
    		List<SubgraphPort> subgraphInputPorts = context.getGraph().getSubgraphInputPorts().getPorts();
    		if (portIndex < 0 || portIndex >= subgraphInputPorts.size()) {
    			throw new IllegalArgumentException("Input port with index " + portIndex + " does not exists.");
    		}
    		return subgraphInputPorts.get(portIndex).isConnected();
    	} else {
    		throw new IllegalStateException("Function can be invoked only within subgraph.");
    	}
    }
    
	static class IsSubgraphInputPortConnectedFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
			Integer portIndex = stack.popInt();

    		stack.push(isSubgraphInputPortConnected(context, portIndex));
    	}
    }

    // IS SUBGRAPH OUTPUT PORT CONNECTED
	@TLFunctionAnnotation("Returns true if specified output port is connected")
    public static boolean isSubgraphOutputPortConnected(TLFunctionCallContext context, int portIndex) {
    	if (context.getGraph().getStaticJobType().isSubJob()) {
    		List<SubgraphPort> subgraphOutputPorts = context.getGraph().getSubgraphOutputPorts().getPorts();
    		if (portIndex < 0 || portIndex >= subgraphOutputPorts.size()) {
    			throw new IllegalArgumentException("Output port with index " + portIndex + " does not exists.");
    		}
    		return subgraphOutputPorts.get(portIndex).isConnected();
    	} else {
    		throw new IllegalStateException("Function can be invoked only within subgraph.");
    	}
    }
    
	static class IsSubgraphOutputPortConnectedFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
			Integer portIndex = stack.popInt();

    		stack.push(isSubgraphOutputPortConnected(context, portIndex));
    	}
    }

	// PARSE PROPERTIES FUNCTION

	/**
	 * @see PropertiesFactory#makeObject(String)
	 * 
	 * @param context
	 * @param input
	 * @return
	 */
	@TLFunctionAnnotation("Converts properties from a string to a map")
	public static final Map<String, String> parseProperties(TLFunctionCallContext context, String input) {
		try {
			return PropertiesUtils.deserialize(input);
		} catch (Exception e) {
			// should never happen
			throw new JetelRuntimeException("Parsing failed", e);
		}
	}
	
	private static class ParsePropertiesFunction extends TLFunctionAdapter {
		
		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(parseProperties(context, stack.popString()));
		}

	}
	

}

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
import org.jetel.graph.GraphParameter;
import org.jetel.graph.GraphParameters;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.HashCodeUtil;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

public class UtilLib extends TLFunctionLibrary {

    @Override
    public TLFunctionPrototype getExecutable(String functionName) {
    	final TLFunctionPrototype ret = 
    		"sleep".equals(functionName) ? new SleepFunction() :
    		"randomUUID".equals(functionName) ? new RandomUuidFunction() : 
           	"getParamValue".equals(functionName) ? new GetParamValueFunction() :
        	"getParamValues".equals(functionName) ? new GetParamValuesFunction() :
           	"getRawParamValue".equals(functionName) ? new GetRawParamValueFunction() :
           	"getRawParamValues".equals(functionName) ? new GetRawParamValuesFunction() :
        	"getJavaProperties".equals(functionName) ? new GetJavaPropertiesFunction() :
    		"getEnvironmentVariables".equals(functionName) ? new GetEnvironmentVariablesFunction() : 
        	"getComponentProperty".equals(functionName) ? new GetComponentPropertyFunction() : 
    		"hashCode".equals(functionName)	? new HashCodeFunction() :	null; 
    		
		if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    
		return ret;
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
		}
    }

    class SleepFunction implements TLFunctionPrototype {

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
    
    class RandomUuidFunction implements TLFunctionPrototype {
    	
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

    class GetParamValueFunction implements TLFunctionPrototype {
    	
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
    
    class GetRawParamValueFunction implements TLFunctionPrototype {
    	
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
		for (GraphParameter param : parameters.getAllGraphParameters()) {
			map.put(param.getName(), refResolver.getResolvedPropertyValue(param.getName(), RefResFlag.SPEC_CHARACTERS_OFF));
		}
		context.setCache(new TLObjectCache<Map<String, String>>(Collections.unmodifiableMap(map)));
    }

    class GetParamValuesFunction implements TLFunctionPrototype {
    	
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

    class GetRawParamValuesFunction implements TLFunctionPrototype {
    	
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
    
    class GetJavaPropertiesFunction implements TLFunctionPrototype {
    	
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
    
    class GetEnvironmentVariablesFunction implements TLFunctionPrototype {
    	
    	@Override
    	public void init(TLFunctionCallContext context) {
    	}
    	
    	@Override
    	public void execute(Stack stack, TLFunctionCallContext context) {
    		stack.push(getEnvironmentVariables(context));
    	}
    }
    
    // GET COMPONENT PROPERTY
    @TLFunctionAnnotation("Returns a map of environment variables. The map is unmodifiable.")
    public static String getComponentProperty(TLFunctionCallContext context, String name) {
    	Node node = context.getTransformationContext().getNode();
    	if (node == null || node.getAttributes() == null) {
    		throw new IllegalStateException("Component properties are not available");
    	}
		return node.getAttributes().getProperty(name);
    }
    
    class GetComponentPropertyFunction implements TLFunctionPrototype {
    	
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
    
      
    class HashCodeFunction implements TLFunctionPrototype {

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
    
}

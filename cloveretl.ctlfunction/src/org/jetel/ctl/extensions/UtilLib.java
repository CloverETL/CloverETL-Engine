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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.jetel.ctl.Stack;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;

public class UtilLib extends TLFunctionLibrary {

    @Override
    public TLFunctionPrototype getExecutable(String functionName) {
    	final TLFunctionPrototype ret = 
    		"sleep".equals(functionName) ? new SleepFunction() :
    		"randomUUID".equals(functionName) ? new RandomUuidFunction() : 
           	"getParamValue".equals(functionName) ? new GetParamValueFunction() :
        	"getParamValues".equals(functionName) ? new GetParamValuesFunction() :
        	"getJavaProperties".equals(functionName) ? new GetJavaPropertiesFunction() :
    		"getEnvironmentVariables".equals(functionName) ? new GetEnvironmentVariablesFunction() : null; 
    		
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
		return ((TLPropertyRefResolverCache) context.getCache()).getCachedPropertyRefResolver().resolveRef(context.getGraph().getGraphProperties().getProperty(paramName), RefResFlag.SPEC_CHARACTERS_OFF);
    }
    
    @TLFunctionInitAnnotation()
    public static final void getParamValueInit(TLFunctionCallContext context) {
		TypedProperties props = context.getGraph() != null ? context.getGraph().getGraphProperties() : null;
		PropertyRefResolver refResolver = new PropertyRefResolver(props);
		
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

    // GET PARAM VALUES
    @SuppressWarnings("unchecked")
	@TLFunctionAnnotation("Returns an unmodifiable map of resolved values of graph parameters")
    public static Map<String, String> getParamValues(TLFunctionCallContext context) {
		return ((TLObjectCache<Map<String, String>>) context.getCache()).getObject();
    }
    
    @TLFunctionInitAnnotation()
    public static final void getParamValuesInit(TLFunctionCallContext context) {
		TypedProperties props = context.getGraph() != null ? context.getGraph().getGraphProperties() : null;
		PropertyRefResolver refResolver = new PropertyRefResolver(props);
		
		Map<String, String> map = new HashMap<String, String>();
		if (props != null) {
			for (String key: props.stringPropertyNames()) {
				map.put(key, refResolver.resolveRef(props.getProperty(key), RefResFlag.SPEC_CHARACTERS_OFF));
			}
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
}

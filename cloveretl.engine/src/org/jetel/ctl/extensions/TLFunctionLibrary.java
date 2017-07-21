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

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.ctl.data.TLType;

/**
 * Recommended ascendant of all TL function libraries. This class is intended
 * to be subclassed by function libraries defined in external engine plugins.
 *  
 * @author Martin Zatopek (martin.zatopek@javlin.cz)
 * 		   Michal Tomcanyi (michal.tomcanyi@javlin.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 30.5.2007
 */
public abstract class TLFunctionLibrary implements ITLFunctionLibrary {

	final Log logger = LogFactory.getLog(TLFunctionLibrary.class);

    protected Map<String, List<TLFunctionDescriptor>> library;
    
    public TLFunctionLibrary() {
        library = new HashMap<String, List<TLFunctionDescriptor>>();
    }

    
    @Override
	public Map<String, List<TLFunctionDescriptor>> getAllFunctions() {
    	return Collections.unmodifiableMap(library);
    }
    
    abstract public String getName();
    
    public String getLibraryClassName() {
    	return getClass().getName();
    }
    
    /**
     * Allocates executable proxy object for given function existing within this library
     * @param functionName
     * @return
     * @throws IllegalArgumentException when function proxy cannot be created
     */
    public abstract TLFunctionPrototype getExecutable(String functionName) 
    throws IllegalArgumentException;
 
    private void registerFunction(TLFunctionDescriptor prototype) {
    	List<TLFunctionDescriptor> registration = library.get(prototype.getName());
    	if (registration == null) {
    		registration = new LinkedList<TLFunctionDescriptor>();
    		library.put(prototype.getName(),registration);
    	}
    	registration.add(prototype);
    }
    
    @Override
	public void init() {
    	Class<? extends TLFunctionLibrary> clazz = getClass();
    	HashSet<String> initMethods = new HashSet<String>();
    	
    	for (Method m : clazz.getMethods()) {
    		if (m.getAnnotation(TLFunctionInitAnnotation.class) != null) {
        		Type[] parameters = m.getGenericParameterTypes();
        		if (parameters.length != 1 || !TLFunctionCallContext.class.equals(parameters[0])) {
        			throw new IllegalArgumentException("Init function definition must have exactly one parameter of type TLFunctionCallContext - method " + m.getName());			
        	    }

    			initMethods.add(m.getName());	
    		}
    	}
    	
    	TLFunctionAnnotation a = null;
    	for (Method m : clazz.getMethods()) {
    		if ( (a = m.getAnnotation(TLFunctionAnnotation.class)) == null) {
        		if (logger.isTraceEnabled()) {
        			logger.trace("Method " + m.toString() + " doesn't represent ctl function (annotation not found). Skipped.");
        		}
    			continue;
    		}
    	
    		String[] paramsDesc = null; // FIXME TLFunctionParametersAnnotation

    		try {
	    		final String functionName = m.getName();
	    		
	    		// extract possible method type parameters for case like:
	    		// e.g. public static <E> List<E> clear(List<E> list)
	    		final List<Type> typeVariables = new LinkedList<Type>();
	    		for (Type t : m.getTypeParameters()) {
	    			typeVariables.add(t);
	    		}
	    		
	    		
	    		boolean isGenericMethod = typeVariables.size() > 0;
	    		
	    		/*
	    		 * Convert return type and formal parameters
	    		 */
	    		final Type javaRetType = m.getGenericReturnType();
	    		
	    		Type[] javaFormal = m.getGenericParameterTypes(); 
	    		Type[] toConvert = new Type[javaFormal.length+1];
	    		toConvert[0] = javaRetType;
	    		System.arraycopy(javaFormal, 0, toConvert, 1, javaFormal.length);
	    		
	    		if (toConvert.length <= 1 || !TLFunctionCallContext.class.equals(toConvert[1])) {
	    			throw new IllegalArgumentException("Java function definition must have TLFunctionCallContext as a first formal parameter.");
	    		}
	    		
	    		TLType[] converted = new TLType[toConvert.length];
	    		int j = 0;
	    		for (int i = 0; i < toConvert.length; i++) {
	    			if (i != 1) // we skip first argument (0-th is the return type) 
	    				converted[j++] = TLType.fromJavaType(toConvert[i]);
				}

	    		TLType returnType = converted[0];
	    		TLType[] formal = new TLType[javaFormal.length-1]; // we're skipping first java formal parameter (with TLFunctionCallContext)
	    		System.arraycopy(converted, 1, formal, 0, formal.length);
	    		
	    		TLFunctionDescriptor descriptor = new TLFunctionDescriptor(this,functionName,a.value(),formal,paramsDesc,returnType,isGenericMethod,m.isVarArgs(), 
	    				initMethods.contains(m.getName() + "Init"));
	    		if (m.getAnnotation(Deprecated.class) != null) {
	    			descriptor.setDeprecated(true);
	    		}
	    		registerFunction(descriptor);
        		if (logger.isTraceEnabled()) {
        			logger.trace("Method " + m.toString() + " registered as ctl function: " + functionName);
        		}
    		} catch (IllegalArgumentException e) {
    			logger.warn("Function '" + getClass().getName() + "." + m.getName() + "' ignored - " + e.getMessage());
    		}
    		
    	}
    }


	@SuppressWarnings("unchecked")
	public static <E> List<E> convertTo(List<Object> list) {
		final List<E> ret = new ArrayList<E>(list.size());
		for (Object o : list) {
			ret.add((E)o);
		}
		return ret;
	}
    
    
 }

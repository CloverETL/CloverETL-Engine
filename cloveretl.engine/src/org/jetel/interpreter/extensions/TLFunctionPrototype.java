/*
 * jETeL/Clover - Java based ETL application framework.
 * Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
package org.jetel.interpreter.extensions;

import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;

public abstract class TLFunctionPrototype {

    // mapping of token types to standard which will be used for checking parameter correctnes
    
    // ??? setting of "static" parameters (in some init() method) - static are all primitive literals
    // won't change during call - ? this is execution context - will be stored together
    // with function call AstNode - this will be something function creates during init()
    // and stores  
    
    protected String name;
    protected String library;
    protected String description;
    protected TLValueType[] parameterTypes;
    protected TLValueType returnType;
    protected int maxParams;
    protected int minParams;
    
    protected TLFunctionPrototype(String library,String name,String description,TLValueType[] parameterTypes,
            TLValueType returnType,int maxParams,int minParams) {
        this.name=name;
        this.library=library;
        this.description=description;
        this.parameterTypes=parameterTypes;
        this.returnType=returnType;
        this.maxParams=maxParams;
        this.minParams=minParams;
    }
    
    protected TLFunctionPrototype(String library,String name,TLValueType[] parameterTypes,
            TLValueType returnType) {
        this(library,name,null,parameterTypes,returnType,parameterTypes.length,parameterTypes.length);
    }
    
    protected TLFunctionPrototype(String library,String name,TLValueType[] parameterTypes,
            TLValueType returnType,int maxParams,int minParams) {
    	this(library,name,null,parameterTypes,returnType,maxParams,minParams);
    }
    
    protected TLFunctionPrototype(String library,String name,String description,TLValueType[] parameterTypes,
            TLValueType returnType) {
    	this(library,name,description,parameterTypes,returnType,parameterTypes.length,parameterTypes.length);
    }
    
    
    public void validateParameters(TLValue[] params) throws ParameterTypeException {
        if (params.length<parameterTypes.length || 
                (maxParams!=-1 && params.length>maxParams)) {
            throw new ParameterTypeException(this,"invalid number of parames");
        }
        
        for(int i=0;i<params.length;i++) {
            if (parameterTypes[i]!=params[i].getType()) {
                throw new ParameterTypeException(this,"invalid parameter types");
            }
        }
    }
    
    public abstract TLValue execute(TLValue[] params, TLContext<?> context);
    
    public String getName() {
        return name;
    }
    
    public TLValueType[] getParameterTypes() {
        return parameterTypes;
    }
    
     
    @Override 
    public int hashCode() {
        return name.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TLFunctionPrototype) {
            return ((TLFunctionPrototype)obj).name.equals(name);
        }
        return false;
    }


    /**
     * @return the library
     * @since 2.4.2007
     */
    public String getLibrary() {
        return library;
    }


    /**
     * @return the returnType
     * @since 2.4.2007
     */
    public TLValueType getReturnType() {
        return returnType;
    }
    
    public TLContext<?> createContext() {
        return TLContext.createEmptyContext();
    }

    /**
     * @return the maxParams
     * @since 29.5.2007
     */
    public int getMaxParams() {
        return maxParams;
    }

	public int getMinParams() {
		return minParams;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}

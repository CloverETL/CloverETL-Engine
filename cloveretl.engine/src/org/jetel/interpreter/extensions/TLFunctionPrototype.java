/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 20.3.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter.extensions;

import org.jetel.interpreter.Stack;
import org.jetel.interpreter.data.TLContext;
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
    protected TLValueType[] parameterTypes;
    protected TLValueType returnType;
    protected int maxParams;
    
    protected TLFunctionPrototype(String library,String name,TLValueType[] parameterTypes,
            TLValueType returnType,int maxParams) {
        this.name=name;
        this.library=library;
        this.parameterTypes=parameterTypes;
        this.returnType=returnType;
        this.maxParams=maxParams;
    }
    
    protected TLFunctionPrototype(String library,String name,TLValueType[] parameterTypes,
            TLValueType returnType) {
        this(library,name,parameterTypes,returnType,-1);
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
    
    public abstract TLValue execute(TLValue[] params, TLContext context);
    
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
    
    public TLContext createContext() {
        return new TLContext<Object>();
    }
}

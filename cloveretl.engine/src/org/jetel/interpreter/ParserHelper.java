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
package org.jetel.interpreter;

import java.util.HashMap;
import java.util.Map;

import org.jetel.interpreter.extensions.TLFunctionFactory;
import org.jetel.interpreter.extensions.TLFunctionPrototype;

/**
 * @author David Pavlis <david.pavlis@javlinconsulting.cz>
 * @since  28.5.2006
 *
 */
public class ParserHelper {

    public static class VarDeclaration {
        public int type;
        public int slot;
        public String name;

        VarDeclaration(String name, int type,int slot) {
        	this.name = name;
            this.type = type;
            this.slot = slot;
        }
    }
    
     static final int INITIAL_HASH_MAP_SIZE=64;
    
     Map<String,VarDeclaration> globalVariableSymbol;
     Map<String,VarDeclaration> localVariableSymbol;
     Map functionSymbol;
     int globalVariableSlotCounter;
     int localVariableSlotCounter;
     boolean inFunctionDeclaration;
     String functionName;
    
    
    /**
     * 
     */
    public ParserHelper() {
        globalVariableSymbol=new HashMap<String,VarDeclaration>(INITIAL_HASH_MAP_SIZE);
        localVariableSymbol=new HashMap<String,VarDeclaration>(INITIAL_HASH_MAP_SIZE);
        functionSymbol=new HashMap(INITIAL_HASH_MAP_SIZE);
        globalVariableSlotCounter=0;
        localVariableSlotCounter=0;
        inFunctionDeclaration=false;
    }
    
    public void reset(){
    	globalVariableSymbol.clear();
    	localVariableSymbol.clear();
    	functionSymbol.clear();
    	globalVariableSlotCounter=0;
    	localVariableSlotCounter=0;
    	inFunctionDeclaration=false;
    }

    public int getNewGlobalSlot(){
           return globalVariableSlotCounter++; 
    }
    
    public int getNewLocalSlot(){
        return localVariableSlotCounter++; 
    }
    
    public boolean addVariable(String name,int type){
        if (inFunctionDeclaration){
            return addLocalVariable(name,type);
        }
        return addGlobalVariable(name,type);
    }
    
    public boolean addGlobalVariable(String name,int type){
        if (globalVariableSymbol.containsKey(name)){
            return false;
        }
        globalVariableSymbol.put(name,new VarDeclaration(name,type,getNewGlobalSlot()));
        return true;
    }
    
    public boolean addLocalVariable(String name,int type){
        if (localVariableSymbol.containsKey(name)){
            return false;
        }
        localVariableSymbol.put(name,new VarDeclaration(name,type,getNewLocalSlot()));
        return true;
    }
    
    public int getGlobalVariableSlot(String name){
        VarDeclaration varDecl=globalVariableSymbol.get(name);
        return varDecl!=null ?  varDecl.slot : -1;
    }
    
    public int getLocalVariableSlot(String name){
        VarDeclaration varDecl=localVariableSymbol.get(name);
        return varDecl!=null ? varDecl.slot: -1;
    }
    
    public int getGlobalVariableType(String name){
        VarDeclaration varDecl=globalVariableSymbol.get(name);
        return varDecl!=null ?  varDecl.type : -1;
    }
    
    public int getLocalVariableType(String name){
        VarDeclaration varDecl=localVariableSymbol.get(name);
        return varDecl!=null ? varDecl.type: -1;
    }
    
    public void enteredFunctionDeclaration(String name){
        inFunctionDeclaration=true;
        functionName=name;
        localVariableSlotCounter=0;
        localVariableSymbol.clear();
    }
  
    public void exitedFunctionDeclaration(){
        inFunctionDeclaration=false;
        functionName=null;
    }
    
    public TLFunctionPrototype getExternalFunction(String name) {
       return TLFunctionFactory.getFunction(name);
    }
    
    public boolean isExternalFunction(String name) {
        return TLFunctionFactory.exists(name);
     }
    
    void dumpExternalFunctions() {
        TLFunctionFactory.dump();
    }

}

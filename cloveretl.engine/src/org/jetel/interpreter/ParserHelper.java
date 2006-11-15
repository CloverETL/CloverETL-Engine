/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-06  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 28.5.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author david
 * @since  28.5.2006
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class ParserHelper {

     static final int INITIAL_HASH_MAP_SIZE=64;
    
     Map globalVariableSymbol;
     Map localVariableSymbol;
     Map functionSymbol;
     int globalVariableSlotCounter;
     int localVariableSlotCounter;
     boolean inFunctionDeclaration;
     String functionName;
    
    /**
     * 
     */
    public ParserHelper() {
        globalVariableSymbol=new HashMap(INITIAL_HASH_MAP_SIZE);
        localVariableSymbol=new HashMap(INITIAL_HASH_MAP_SIZE);
        functionSymbol=new HashMap(INITIAL_HASH_MAP_SIZE);
        globalVariableSlotCounter=0;
        localVariableSlotCounter=0;
        inFunctionDeclaration=false;
    }

    public Integer getNewGlobalSlot(){
           return new Integer(globalVariableSlotCounter++); 
    }
    
    public Integer getNewLocalSlot(){
        return new Integer(localVariableSlotCounter++); 
    }
    
    public boolean addVariable(String name){
        if (inFunctionDeclaration){
            return addLocalVariable(name);
        }else{
            return addGlobalVariable(name);
        }
    }
    
    public boolean addGlobalVariable(String name){
        if (globalVariableSymbol.containsKey(name)){
            return false;
        }
        globalVariableSymbol.put(name,getNewGlobalSlot());
        return true;
    }
    
    public boolean addLocalVariable(String name){
        if (localVariableSymbol.containsKey(name)){
            return false;
        }
        localVariableSymbol.put(name,getNewLocalSlot());
        return true;
    }
    
    public int getGlobalVariableSlot(String name){
        Integer slot=(Integer)globalVariableSymbol.get(name);
        return slot!=null ? slot.intValue() : -1;
    }
    
    public int getLocalVariableSlot(String name){
        Integer slot=(Integer)localVariableSymbol.get(name);
        return slot!=null ? slot.intValue() : -1;
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
}

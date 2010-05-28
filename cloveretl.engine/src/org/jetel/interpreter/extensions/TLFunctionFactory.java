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
package org.jetel.interpreter.extensions;

import java.util.HashMap;
import java.util.Map;


public class TLFunctionFactory {
    
//    public static String DEFAULT_LIBRARY = "DEF_LIB";
    
    private static Map<String,TLFunctionPrototype> functionLib=new HashMap<String,TLFunctionPrototype>(64);
    
    private TLFunctionFactory() {
    }
    
    public static boolean exists(String functionName) {
        if(functionLib.containsKey(functionName)) {
            return true;
        } else {
            return TLFunctionPluginRepository.exists(functionName);
        }
    }

    public static TLFunctionPrototype getFunction(String functionName) {
        TLFunctionPrototype function = functionLib.get(functionName);
        if(function == null) {
            function = TLFunctionPluginRepository.getFunction(functionName);
            registerFunction(function);
        }
        return function;
    }

    public static void registerFunction(TLFunctionPrototype function){
        functionLib.put(function.name, function);
    }

    
//    public static boolean exists(String library,String functionName) {
//        return functionLib.containsKey(library+"."+functionName);
//    }
//    
//    public static boolean exists(String functionName) {
//        return exists(DEFAULT_LIBRARY,functionName);
//    }
//    
//    public static TLFunctionPrototype getFunction(String library,String functionName){
//        TLFunctionPrototype function =functionLib.get(library+"."+functionName);
//       return function;
//    }
//    
//    public static TLFunctionPrototype getFunction(String functionName){
//        return getFunction(DEFAULT_LIBRARY,functionName);
//    }
//        
//    public static void registerFunction(String library,TLFunctionPrototype function){
//        functionLib.put(library+"."+function.name, function);
//    }
//    
//    public static void registerFunction(TLFunctionPrototype function){
//        registerFunction(DEFAULT_LIBRARY,function);
//     }

    public static void dump() {
        System.out.println("#registered functions: "+functionLib.size());
        for(String function: functionLib.keySet()) {
            System.out.println(function);
        }
    }
    
    public static Map<String,TLFunctionPrototype> registerAndGetAllFunctions() {
    	
    	for(TLFunctionPrototype function : TLFunctionPluginRepository.getAllFunctions()) {
    		registerFunction(function);
    	}
    	return functionLib;
    	
    }
    
}

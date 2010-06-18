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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jetel.graph.dictionary.StringDictionaryType;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLContainerValue;
import org.jetel.interpreter.data.TLListValue;
import org.jetel.interpreter.data.TLMapValue;
import org.jetel.interpreter.data.TLNullValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;

public class ContainerLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Container";

    enum Function {
        REMOVEALL("remove_all"), PUSH("push"), POP("pop"), POLL("poll"), INSERT("insert"), REMOVE("remove"), SORT("sort") ,
        COPY("copy"), REVERSE("reverse"), DICTIONARY_GET_STRING("dict_get_str"), DICTIONARY_PUT_STRING("dict_put_str");
        
        public String name;
        
        private Function(String name) {
            this.name = name;
        }
        
        public static Function fromString(String s) {
            for(Function function : Function.values()) {
                if(s.equalsIgnoreCase(function.name) || s.equalsIgnoreCase(LIBRARY_NAME + "." + function.name)) {
                    return function;
                }
            }
            return null;
        }
    }

    public ContainerLib() {
        super();
     }

    public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case REMOVEALL: return new RemoveAllFunction();
        case PUSH:  return new PushFunction();
        case POP: return new PopFunction();
        case POLL: return new PollFunction();
        case INSERT: return new InsertFunction();
        case REMOVE: return new RemoveFunction();
        case SORT: return new SortFunction();
        case COPY: return new CopyFunction();
        case REVERSE: return new ReverseFunction();
        case DICTIONARY_GET_STRING: return new DictGetStrFunction();
        case DICTIONARY_PUT_STRING: return new DictPutStrFunction();
        default: return null;
       }
    }
    
    public  Collection<TLFunctionPrototype> getAllFunctions() {
    	List<TLFunctionPrototype> ret = new ArrayList<TLFunctionPrototype>();
    	Function[] fun = Function.values();
    	for (Function function : fun) {
    		ret.add(getFunction(function.name));
		}
    	
    	return ret;
    }

    // CLEAR
    class RemoveAllFunction extends TLFunctionPrototype {
    	
    	public RemoveAllFunction(){
    		super("container", "remove_all", "Removes all items from list/map.",
    				new TLValueType[] { TLValueType.OBJECT }, TLValueType.BOOLEAN);
    	}
    
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
        	if (!params[0].type.isArray()){
        		throw new TransformLangExecutorRuntimeException(params,
				"remove_all - wrong type of literal");
        	}
        	((TLContainerValue)params[0]).clear();
        	return  TLBooleanValue.TRUE;
        }
        
        @Override
        public TLContext createContext() {
            return TLContext.createNullContext();
        }
    }
    
    
    // POP
	class PopFunction extends TLFunctionPrototype {

		public PopFunction() {
			super("container", "pop", "Removes last element from a list and returns it.",
					new TLValueType[] { TLValueType.LIST }, TLValueType.OBJECT);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type != TLValueType.LIST) {
				throw new TransformLangExecutorRuntimeException(params,
						"pop - wrong type of literal");
			}
			List<TLValue> mylist=((TLListValue)params[0]).getList();
			return mylist.size()>0 ? mylist.remove(mylist.size()-1) : TLNullValue.getInstance();
		}

		@Override
		public TLContext createContext() {
			return TLContext.createNullContext();
		}

	}

	// POLL
	class PollFunction extends TLFunctionPrototype {

		public PollFunction() {
			super("container", "poll", "Removes first element from a list and returns it.",
					new TLValueType[] { TLValueType.LIST }, TLValueType.OBJECT);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type != TLValueType.LIST) {
				throw new TransformLangExecutorRuntimeException(params,
						"poll - wrong type of literal");
			}
			List<TLValue> mylist=((TLListValue)params[0]).getList();
			return mylist.size()>0 ? mylist.remove(0) : TLNullValue.getInstance();
		}

		@Override
		public TLContext createContext() {
			return TLContext.createNullContext();
		}

	}
	
	// PUSH
	class PushFunction extends TLFunctionPrototype {

		public PushFunction() {
			super("container", "push", "Adds element at the end of list.",
					new TLValueType[] { TLValueType.LIST, TLValueType.OBJECT }, TLValueType.BOOLEAN);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type != TLValueType.LIST) {
				throw new TransformLangExecutorRuntimeException(params,
						"push - wrong type of literal(s)");
			}
			List<TLValue> mylist=((TLListValue)params[0]).getList();
			mylist.add(params[1].duplicate());
			return TLBooleanValue.TRUE;
		}

		@Override
		public TLContext createContext() {
			return TLContext.createNullContext();
		}

	}

	// INSERT
	class InsertFunction extends TLFunctionPrototype {

		public InsertFunction() {
			super("container", "insert", "Inserts element(s) at the specified index.",
					new TLValueType[] { TLValueType.LIST, TLValueType.INTEGER, TLValueType.OBJECT }, TLValueType.BOOLEAN , 999 , 3);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type != TLValueType.LIST || ( ! params[1].type.isNumeric())) {
				throw new TransformLangExecutorRuntimeException(params,
						"insert - wrong type of literal(s)");
			}
			List<TLValue> mylist=((TLListValue)params[0]).getList();
			int index=params[1].getNumeric().getInt();
			if (mylist.size()<index || index<0 ) return TLBooleanValue.FALSE;
			for (int i=2; i< params.length; mylist.add(index++, params[i++]));
			return TLBooleanValue.TRUE;
		}

		@Override
		public TLContext createContext() {
			return TLContext.createNullContext();
		}

	}
	
	// REMOVE
	class RemoveFunction extends TLFunctionPrototype {

		public RemoveFunction() {
			super("container", "remove", "Removes element at the specified index and returns it.",
					new TLValueType[] { TLValueType.LIST, TLValueType.INTEGER }, TLValueType.OBJECT);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type != TLValueType.LIST || ( ! params[1].type.isNumeric())) {
				throw new TransformLangExecutorRuntimeException(params,
						"remove - wrong type of literal(s)");
			}
			List<TLValue> mylist=((TLListValue)params[0]).getList();
			int index=params[1].getNumeric().getInt();
			return (index>=0 && index < mylist.size()) ? mylist.remove(index) : TLNullValue.getInstance();
		}

		@Override
		public TLContext createContext() {
			return TLContext.createNullContext();
		}

	}
	
	// SORT
	class SortFunction extends TLFunctionPrototype {

		public SortFunction() {
			super("container", "sort", "Sorts elements contained in list - ascending order.",
					new TLValueType[] { TLValueType.LIST }, TLValueType.LIST);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type != TLValueType.LIST ) {
				throw new TransformLangExecutorRuntimeException(params,
						"sort - wrong type of literal(s)");
			}
			List<TLValue> mylist=((TLListValue)params[0]).getList();
			Collections.sort(mylist);
			return params[0];
		}

		@Override
		public TLContext createContext() {
			return TLContext.createNullContext();
		}

	}
	
	// REVERSE
	class ReverseFunction extends TLFunctionPrototype {

		public ReverseFunction() {
			super("container", "reverse", "Reverses order of elements contained in list.",
					new TLValueType[] { TLValueType.LIST }, TLValueType.LIST);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type != TLValueType.LIST ) {
				throw new TransformLangExecutorRuntimeException(params,
						"reverse - wrong type of literal(s)");
			}
			List<TLValue> mylist=((TLListValue)params[0]).getList();
			Collections.reverse(mylist);
			return params[0];
		}

		@Override
		public TLContext createContext() {
			return TLContext.createNullContext();
		}

	}
	
	// COPY
	class CopyFunction extends TLFunctionPrototype {

		public CopyFunction() {
			super("container", "copy", "Copies content of 2nd parameter into 1st (both containers).",
					new TLValueType[] { TLValueType.OBJECT, TLValueType.OBJECT }, TLValueType.OBJECT);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if ( !params[0].type.isArray() || !params[0].type.isArray() ) {
				throw new TransformLangExecutorRuntimeException(params,
						"copy - wrong type of literal(s)");
			}
			if (params[0].type == TLValueType.LIST){
				List<TLValue> dest=((TLListValue)params[0]).getList();
				dest.addAll(((TLContainerValue)params[1]).getCollection());
			} else if (params[0].type == TLValueType.MAP && params[1].type==TLValueType.MAP){
				Map dest = ((TLMapValue)params[0]).getMap();
				dest.putAll(((TLMapValue)params[1]).getMap());
			}else {
				throw new TransformLangExecutorRuntimeException(params,
				"copy - incompatible literals");
			}
			return params[0];
		}

		@Override
		public TLContext createContext() {
			return TLContext.createNullContext();
		}

	}
	
	class DictGetStrFunction extends TLFunctionPrototype {
		
		public DictGetStrFunction(){
			super("container", "dict_get_str", "Returns string representation of dictionary object under specified name/key.",
					new TLValueType[] { TLValueType.STRING }, TLValueType.STRING);
		}
		
		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type!= TLValueType.STRING){
				throw new TransformLangExecutorRuntimeException(params,
				this.name+"- wrong type of literal(s)");
			}
			TLStringValue value=(TLStringValue)context.getContext();
			Object content=context.graph.getDictionary().getValue(params[0].toString());
			if (content instanceof String){
				value.setValue(content);
				return value;
			}else{
				return TLNullValue.getInstance();
			}
		}
		
		@Override
		public TLContext createContext() {
			return TLContext.createStringContext();
		}
	}
	

	class DictPutStrFunction extends TLFunctionPrototype {
		
		public DictPutStrFunction(){
			super("container", "dict_put_str", "Stores value passed as 2nd parameter under key (1st parameter) into graph dictionary.",
					new TLValueType[] { TLValueType.STRING , TLValueType.STRING }, TLValueType.BOOLEAN);
		}
		
		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			if (params[0].type!= TLValueType.STRING || params[1].type!= TLValueType.STRING){
				throw new TransformLangExecutorRuntimeException(params,
				this.name+"- wrong type of literal(s)");
			}
		
			try{
				context.graph.getDictionary().setValue(params[0].toString(),StringDictionaryType.TYPE_ID,params[1].toString());
				return TLBooleanValue.TRUE;
			}catch(Exception ex){
				return TLBooleanValue.FALSE;
			}
		}
		
		@Override
		public TLContext createContext() {
			return TLContext.createStringContext();
		}
	}
}

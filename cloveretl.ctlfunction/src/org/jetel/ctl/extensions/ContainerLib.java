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
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jetel.ctl.Stack;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLType.TLTypeList;

public class ContainerLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "Container";

    
    @Override
    public TLFunctionPrototype getExecutable(String functionName) 
    throws IllegalArgumentException {
    	TLFunctionPrototype ret = 
    		"clear".equals(functionName) ? new ClearFunction() :
    		"pop".equals(functionName) ? new PopFunction() :
    		"poll".equals(functionName) ? new PollFunction() :
    		"push".equals(functionName) ? new PushFunction() :
    		"append".equals(functionName) ? new AppendFunction() :
    		"insert".equals(functionName) ? new InsertFunction() :
    		"remove".equals(functionName) ? new RemoveFunction() :
    		"sort".equals(functionName) ? new SortFunction() : 
    		"reverse".equals(functionName) ? new ReverseFunction() : 
    		"isEmpty".equals(functionName) ? new IsEmptyFunction() : 
    		"copy".equals(functionName) ? new CopyFunction() : null;

    	if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    	
    	return ret;
    }
    
    
    // REMOVE_ALL
    @TLFunctionAnnotation("Removes all elements from a list")
    public static final <E> void clear(TLFunctionCallContext context, List<E> list) {
    	list.clear();
    }
    @TLFunctionAnnotation("Removes all elements from a list")
    public static final <K,V> void clear(TLFunctionCallContext context, Map<K,V> map) {
    	map.clear();
    }
    
    class ClearFunction implements TLFunctionPrototype {
    	
		public void init(TLFunctionCallContext context) {
		}

    	public void execute(Stack stack, TLFunctionCallContext context) {
    		if (context.getParams()[0].isList()) {
    			clear(context, stack.popList());
    		} else {
    			clear(context, stack.popMap());
    		}
    	}
    }
    
    
    // POP
    @TLFunctionAnnotation("Removes last element from a list and returns it.")
    public static final <E> E pop(TLFunctionCallContext context, List<E> list) {
    	return list.size() > 0 ? list.remove(list.size()-1) : null;
    }
	class PopFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}


		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(pop(context, stack.popList()));
		}
	}

	// POLL
	@TLFunctionAnnotation("Removes first element from a list and returns it.")
	public static final <E> E poll(TLFunctionCallContext context, List<E> list) {
		return list.size() > 0 ? list.remove(0) : null;
	}
	class PollFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}


		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(poll(context, stack.popList()));
		}
		
	}
	
	// APPEND
	@TLFunctionAnnotation("Appends element at the end of the list.")
	public static final <E> List<E> append(TLFunctionCallContext context, List<E> list, E item) {
		list.add(item);
		return list;
	}
	
	class AppendFunction implements TLFunctionPrototype {

		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			final Object item = stack.pop();
			final List<Object> list = stack.popList();
			stack.push(append(context, list, item));
		}
	}

	// PUSH
	@TLFunctionAnnotation("Appends element at the end of the list.")
	public static final <E> List<E> push(TLFunctionCallContext context, List<E> list, E item) {
		list.add(item);
		return list;
	}
	
	class PushFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}


		public void execute(Stack stack, TLFunctionCallContext context) {
			final Object item  = stack.pop();
			final List<Object> list = stack.popList();
			stack.push(push(context, list, item));
		}
	}
	

	// INSERT
	@TLFunctionAnnotation("Inserts element at the specified index.")
	public static final <E> List<E> insert(TLFunctionCallContext context, List<E> list, int position, E item) {
		list.add(position,item);
		return list;
	}
	class InsertFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}


		public void execute(Stack stack, TLFunctionCallContext context) {
			final Object item = stack.pop();
			final Integer pos = stack.popInt();
			final List<Object> list = stack.popList();
			stack.push(insert(context, list, pos, item));
		}
	}
	
	// REMOVE
	@TLFunctionAnnotation("Removes element at the specified index and returns it.")
	public static final <E> E remove(TLFunctionCallContext context, List<E> list, int position) {
		return list.remove(position);
	}
	class RemoveFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}


		public void execute(Stack stack, TLFunctionCallContext context) {
			final Integer pos = stack.popInt();
			final List<Object> list = stack.popList();
			stack.push(remove(context, list, pos));
		}
		
	}

	// all CTL types are comparable so this will work in runtime
	@TLFunctionAnnotation("Sorts elements contained in list - ascending order.")
	public static final <E extends Comparable<E>> List<E> sort(TLFunctionCallContext context, List<E> list) { 
		Collections.sort(list);
		return list;
	}
	class SortFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}


		@SuppressWarnings("unchecked")
		public void execute(Stack stack, TLFunctionCallContext context) {
			List<Object> orig = (List<Object>)stack.peek();
			TLType elem = ((TLTypeList)context.getParams()[0]).getElementType();
			
			List<?> s = null;
			
			if (elem.isString()) {
				s = sort(context, TLFunctionLibrary.<String>convertTo(orig));
			} else if (elem.isInteger()) {
				s = sort(context, TLFunctionLibrary.<Integer>convertTo(orig));
			} else if (elem.isLong()) {
				s = sort(context, TLFunctionLibrary.<Long>convertTo(orig));
			} else if (elem.isDouble()) {
				s = sort(context, TLFunctionLibrary.<Double>convertTo(orig));
			} else if (elem.isDecimal()) {
				s = sort(context, TLFunctionLibrary.<BigDecimal>convertTo(orig));
			} else if (elem.isBoolean()) {
				s = sort(context, TLFunctionLibrary.<Boolean>convertTo(orig));
			} else if (elem.isDate()) {
				s = sort(context, TLFunctionLibrary.<Date>convertTo(orig));
			} else {
				throw new IllegalArgumentException("Unknown type for sort: '" + elem.name() + "'");
			}
			
			orig.clear();
			orig.addAll(s);
		}
		
	}
	
		
	// all CTL types are comparable so this will work in runtime
	@TLFunctionAnnotation("Inverts order of elements withing a list.")
	public static final <E extends Comparable<E>> List<E> reverse(TLFunctionCallContext context, List<E> list) { 
		Collections.reverse(list);
		return list;
	}
		
	// REVERSE
	class ReverseFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}

		@SuppressWarnings("unchecked")
		public void execute(Stack stack, TLFunctionCallContext context) {
			List<Object> orig = (List<Object>)stack.peek();
			TLType elem = ((TLTypeList)context.getParams()[0]).getElementType();
			
			List<?> s = null;
			
			if (elem.isString()) {
				s = reverse(context, TLFunctionLibrary.<String>convertTo(orig));
			} else if (elem.isInteger()) {
				s = reverse(context, TLFunctionLibrary.<Integer>convertTo(orig));
			} else if (elem.isLong()) {
				s = reverse(context, TLFunctionLibrary.<Long>convertTo(orig));
			} else if (elem.isDouble()) {
				s = reverse(context, TLFunctionLibrary.<Double>convertTo(orig));
			} else if (elem.isDecimal()) {
				s = reverse(context, TLFunctionLibrary.<BigDecimal>convertTo(orig));
			} else if (elem.isBoolean()) {
				s = reverse(context, TLFunctionLibrary.<Boolean>convertTo(orig));
			} else if (elem.isDate()) {
				s = reverse(context, TLFunctionLibrary.<Date>convertTo(orig));
			} else {
				throw new IllegalArgumentException("Unknown type for reverse: '" + elem.name() + "'");
			}
			
			orig.clear();
			orig.addAll(s);
			
		}
		
	}
	
	// ISEMPTY
	@TLFunctionAnnotation("Checks if list is empty.")
	public static final <E> Boolean isEmpty(TLFunctionCallContext context, List<E> list) {
		return list.isEmpty();
	}
	
	@TLFunctionAnnotation("Checks if map is empty.")
	public static final <K, V> Boolean isEmpty(TLFunctionCallContext context, Map<K, V> map) {
		return map.isEmpty();
	}
	
	class IsEmptyFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isMap()) {
				stack.push(isEmpty(context, stack.popMap()));
			} else {
				stack.push(isEmpty(context, stack.popList()));
			}
			final List<Object> list = stack.popList();
			
		}
	}
	
	// COPY
	@TLFunctionAnnotation("Adds all elements from second argument into the first argument. Returns the first argument")
	public static final <E> List<E> copy(TLFunctionCallContext context, List<E> to, List<E> from) {
		to.addAll(from);
		return to;
	}
	
	@TLFunctionAnnotation("Adds all elements from second argument into the first argument, replacing existing key mappings. Returns the first argument")
	public static final <K,V> Map<K,V> copy(TLFunctionCallContext context, Map<K,V> to, Map<K,V> from) {
		to.putAll(from);
		return to;
	}
	class CopyFunction implements TLFunctionPrototype{
		
		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isList()) {
				List<Object> from = stack.popList();
				List<Object> to = stack.popList();
				stack.push(copy(context, to,from));
			}
			
			if (context.getParams()[0].isMap()) {
				Map<Object,Object> from = stack.popMap();
				Map<Object,Object> to = stack.popMap();
				stack.push(copy(context, to,from));
			}
		}
	}
}

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jetel.ctl.Stack;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLType.TLTypeList;
import org.jetel.data.DataRecord;

public class ContainerLib extends TLFunctionLibrary {
	
    @Override
    public TLFunctionPrototype getExecutable(String functionName) 
    throws IllegalArgumentException {
		if (functionName != null) {
			switch (functionName) {
				case "clear": return new ClearFunction();
				case "pop": return new PopFunction();
				case "poll": return new PollFunction();
				case "push": return new PushFunction();
				case "append": return new AppendFunction();
				case "insert": return new InsertFunction();
				case "remove": return new RemoveFunction();
				case "sort": return new SortFunction();
				case "reverse": return new ReverseFunction();
				case "isEmpty": return new IsEmptyFunction();
				case "copy": return new CopyFunction();
				case "containsAll": return new ContainsAllFunction();
				case "containsKey": return new ContainsKeyFunction();
				case "containsValue": return new ContainsValueFunction();
				case "binarySearch": return new BinarySearchFunction();
				case "getKeys": return new GetKeysFunction();
				case "getValues": return new GetValuesFunction();
				case "toMap": return new ToMapFunction();
			}
		}

    	throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    }
    
	private static String LIBRARY_NAME = "Container";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}

    
    
    // CLEAR
    @TLFunctionAnnotation("Removes all elements from a list")
    public static final <E> void clear(TLFunctionCallContext context, List<E> list) {
    	list.clear();
    }
    @TLFunctionAnnotation("Removes all elements from a map")
    public static final <K,V> void clear(TLFunctionCallContext context, Map<K,V> map) {
    	map.clear();
    }
    
    static class ClearFunction implements TLFunctionPrototype {
    	
		@Override
		public void init(TLFunctionCallContext context) {
		}

    	@Override
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
    static class PopFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}


		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			stack.push(pop(context, stack.popList()));
		}
	}

	// POLL
	@TLFunctionAnnotation("Removes first element from a list and returns it.")
	public static final <E> E poll(TLFunctionCallContext context, List<E> list) {
		return list.size() > 0 ? list.remove(0) : null;
	}
	static class PollFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}


		@Override
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
	
	static class AppendFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
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
	
	static class PushFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}


		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final Object item  = stack.pop();
			final List<Object> list = stack.popList();
			stack.push(push(context, list, item));
		}
	}
	
	// INSERT
	@TLFunctionAnnotation("Inserts elements at the specified index.")
	public static final <E> List<E> insert(TLFunctionCallContext context, List<E> list, int position, E... items) {
		for (int i = 0; i < items.length; i++) {
			list.add(position++, items[i]);
		}
		return list;
	}
	
	@TLFunctionAnnotation("Inserts elements at the specified index.")
	public static final <E> List<E> insert(TLFunctionCallContext context, List<E> list, int position, List<E> items) {
		for (int i = 0; i < items.size(); i++) {
			list.add(position++, items.get(i));
		}
		return list;
	}
	
	static class InsertFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[2].isList()) {
				List<Object> items = stack.popList();
				final Integer pos = stack.popInt();
				final List<Object> list = stack.popList();
				stack.push(insert(context, list, pos, items));
			} else {
				Object[] items = new Object[context.getParams().length - 2];
				for (int i = items.length - 1; i >= 0; i--) {
					items[i] = stack.pop();
				}
				final Integer pos = stack.popInt();
				final List<Object> list = stack.popList();
				stack.push(insert(context, list, pos, items));
			}
			
		}
	}
	
	// REMOVE
	@TLFunctionAnnotation("Removes element at the specified index and returns it.")
	public static final <E> E remove(TLFunctionCallContext context, List<E> list, int position) {
		return list.remove(position);
	}
	static class RemoveFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}


		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			final Integer pos = stack.popInt();
			final List<Object> list = stack.popList();
			stack.push(remove(context, list, pos));
		}
		
	}

	private static class MyComparator<T extends Comparable<T>> implements Comparator<T> {

		@Override
		public int compare(T o1, T o2) {
			if (o1 == o2) {
				return 0;
			}
			if (o1 == null) {
				return 1;
			}
			if (o2 == null) {
				return -1;
			}
			return o1.compareTo(o2);
		}
		
	};
	
	// all CTL types are comparable so this will work in runtime
	@TLFunctionAnnotation("Sorts elements contained in list - ascending order.")
	public static final <E extends Comparable<E>> List<E> sort(TLFunctionCallContext context, List<E> list) { 
		Collections.sort(list, new MyComparator<E>());
		return list;
	}
	static class SortFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}


		@Override
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
	@TLFunctionAnnotation("Inverts order of elements within a list.")
	public static final <E> List<E> reverse(TLFunctionCallContext context, List<E> list) { 
		Collections.reverse(list);
		return list;
	}
		
	// REVERSE
	static class ReverseFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
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
			} else if (elem.isRecord()) {
				s = reverse(context, TLFunctionLibrary.<DataRecord>convertTo(orig));
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
	
	static class IsEmptyFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isMap()) {
				stack.push(isEmpty(context, stack.popMap()));
			} else {
				stack.push(isEmpty(context, stack.popList()));
			}
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
	static class CopyFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
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

	@TLFunctionAnnotation("Checks if a list contains all elements from another list.")
	public static final <E> boolean containsAll(TLFunctionCallContext context, List<E> collection, List<E> subList) {
		return collection.containsAll(subList);
	}
	static class ContainsAllFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isList()) {
				List<Object> subList = stack.popList();
				List<Object> list = stack.popList();
				stack.push(containsAll(context, list, subList));
			}
		}
	}
	
	@TLFunctionAnnotation("Checks if a map contains a specified key.")
	public static final <K, V> boolean containsKey(TLFunctionCallContext context, Map<K, V> map, K key) {
		return map.containsKey(key);
	}
	static class ContainsKeyFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isMap()) {
				Object key = stack.pop();
				Map<Object, Object> map = stack.popMap();
				stack.push(containsKey(context, map, key));
			}
		}
	}
	
	@TLFunctionAnnotation("Checks if a map contains a specified value.")
	public static final <K, V> boolean containsValue(TLFunctionCallContext context, Map<K, V> map, V value) {
		return map.containsValue(value);
	}
	
	@TLFunctionAnnotation("Checks if a list contains a specified value.")
	public static final <V> boolean containsValue(TLFunctionCallContext context, List<V> list, V value) {
		return list.contains(value);
	}
	
	static class ContainsValueFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			TLType type=context.getParams()[0];
			if (type.isMap()) {
				Object value = stack.pop();
				Map<Object, Object> map = stack.popMap();
				stack.push(containsValue(context, map, value));
			}else if (type.isList()){
				Object value = stack.pop();
				List<Object> list = stack.popList();
				stack.push(containsValue(context, list, value));
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	@TLFunctionAnnotation("Searches a list for the specified value. The list must be sorted in ascending order.")
	public static final <V> Integer binarySearch(TLFunctionCallContext context, List<V> list, V value) {
		if (value == null) {
			throw new NullPointerException("value is null");
		} else if (!(value instanceof Comparable)) {
			throw new IllegalArgumentException("value is not comparable");
		}
		return Collections.binarySearch(((List<? extends Comparable<? super V>>) list), value);
	}
	
	static class BinarySearchFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			Object value = stack.pop();
			List<Object> list = stack.popList();
			stack.push(binarySearch(context, list, value));
		}
	}
	
	@TLFunctionAnnotation("Returns the keys of the map.")
	public static final <K, V> List<K> getKeys(TLFunctionCallContext context, Map<K, V> map) {
		return new ArrayList<K>(map.keySet());
	}
	static class GetKeysFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isMap()) {
				Map<Object, Object> map = stack.popMap();
				stack.push(getKeys(context, map));
			}
		}
	}
	
	@TLFunctionAnnotation("Returns the values of the map.")
	public static final <K, V> List<V> getValues(TLFunctionCallContext context, Map<K, V> map) {
		return new ArrayList<V>(map.values());
	}
	static class GetValuesFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[0].isMap()) {
				Map<Object, Object> map = stack.popMap();
				stack.push(getValues(context, map));
			}
		}
	}
	
	@TLFunctionAnnotation("Converts input list to map where list items become map's keys, all values are the same constant.")
	public static final <K, V> Map<K,V> toMap(TLFunctionCallContext context, List<K> keys, V constant) {
		Map <K,V> map = new LinkedHashMap<K,V>(keys.size());
		for(K key: keys){
			map.put(key, constant);
		}
		return map;
		
	}
	
	@TLFunctionAnnotation("Converts two input lists to map where first list items become map's keys and second list corresponding values.")
	public static final <K, V> Map<K,V> toMap(TLFunctionCallContext context, List<K> keys, List<V> values) {
		Map <K,V> map = new LinkedHashMap<K,V>(keys.size());
		Iterator<V> iter = values.iterator();
		if (keys.size()!=values.size()){
			throw new IllegalArgumentException("Keys list does not match values list in size.");
		}
		for(K key: keys){
			map.put(key, iter.next());
		}
		return map;
		
	}
	
	static class ToMapFunction implements TLFunctionPrototype{
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			if (context.getParams()[1].isList()) {
				List<Object> values = stack.popList();
				stack.push(toMap(context, stack.popList(), values));
			}else{
				Object constant = stack.pop();
				stack.push(toMap(context, stack.popList(), constant));
			}
		}
	}
	
}

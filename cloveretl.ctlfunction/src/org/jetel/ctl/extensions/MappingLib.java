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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.ctl.Stack;
import org.jetel.data.Defaults;
import org.jetel.util.string.StringUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 17. 3. 2015
 */
public class MappingLib extends TLFunctionLibrary {

	private static String LIBRARY_NAME = "Mapping";

	@Override
	public TLFunctionPrototype getExecutable(String functionName) throws IllegalArgumentException {
		if (functionName != null) {
			switch (functionName) {
				case "getMappedSourceFields": return new GetMappedSourceFieldsFunction();
				case "getMappedTargetFields": return new GetMappedTargetFieldsFunction();
				case "isTargetFieldMapped": return new IsTargetFieldMappedFunction();
				case "isSourceFieldMapped": return new IsSourceFieldMappedFunction();
			}
		}
		
		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
	}

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}
	
	/*
	 * Pre-compiled patterns
	 */
	private static final Pattern HASH_PATTERN = Pattern.compile("#");
	private static final Pattern SEMICOLON_PATTERN = Pattern.compile(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
	private static final Pattern ASSIGNMENT_PATTERN = Pattern.compile("\\s*=\\s*");
	
	private static final Pattern ORDER_PATTERN = Pattern.compile("(.*)\\([adir]\\)");
	
	// GET MAPPED SOURCE FIELDS
	
    private static final List<String> doGetMappedSourceFields(TLFunctionCallContext context, String mapping, String targetField, Integer sourceIdx) {
    	Mapping m = getMapping(context, mapping);
    	return m.getMappedSourceFields(targetField, sourceIdx);
    }

    @TLFunctionAnnotation("Returns fields from the specified source mapped to the specified target field")
    public static final List<String> getMappedSourceFields(TLFunctionCallContext context, String mapping, String targetField, Integer sourceIdx) {
    	Objects.requireNonNull(sourceIdx, "Source index is null");
    	return doGetMappedSourceFields(context, mapping, targetField, sourceIdx);
    }
    
    @TLFunctionAnnotation("Returns fields mapped to the specified target field")
    public static final List<String> getMappedSourceFields(TLFunctionCallContext context, String mapping, String targetField) {
    	return doGetMappedSourceFields(context, mapping, targetField, null);
    }
    
    @TLFunctionAnnotation("Returns all mapped source fields")
    public static final List<String> getMappedSourceFields(TLFunctionCallContext context, String mapping) {
    	Mapping m = getMapping(context, mapping);
    	return m.getMappedSourceFields();
    }
    
    @TLFunctionInitAnnotation()
    public static final void getMappedSourceFieldsInit(TLFunctionCallContext context){
    	context.setCache(new TLMappingCache(context, 0));
    }
    
    private static class GetMappedSourceFieldsFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getMappedSourceFieldsInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			int paramCount = context.getParams().length;
			if (paramCount == 1) { // only mapping
				stack.push(getMappedSourceFields(context, stack.popString()));
			} else {
				Integer sourceIdx = null;
				if (paramCount > 2) {
					sourceIdx = stack.popInt();
					Objects.requireNonNull(sourceIdx, "Source index is null");
				}
				String target = stack.popString();
				String mapping = stack.popString();
				stack.push(doGetMappedSourceFields(context, mapping, target, sourceIdx));
			}
		} 

    }
    
	// GET MAPPED TARGET FIELDS
	
    private static final List<String> doGetMappedTargetFields(TLFunctionCallContext context, String mapping, String sourceField, Integer sourceIdx) {
    	Mapping m = getMapping(context, mapping);
    	return m.getMappedTargetFields(sourceField, sourceIdx);
    }

    @TLFunctionAnnotation("Returns target fields mapped from the specified source field")
    public static final List<String> getMappedTargetFields(TLFunctionCallContext context, String mapping, String sourceField, Integer sourceIdx) {
    	Objects.requireNonNull(sourceIdx, "Source index is null");
    	return doGetMappedTargetFields(context, mapping, sourceField, sourceIdx);
    }
    
    @TLFunctionAnnotation("Returns target fields mapped from source field with the specified name")
    public static final List<String> getMappedTargetFields(TLFunctionCallContext context, String mapping, String sourceField) {
    	return doGetMappedTargetFields(context, mapping, sourceField, null);
    }
    
    @TLFunctionAnnotation("Returns all target fields mapped from any source field")
    public static final List<String> getMappedTargetFields(TLFunctionCallContext context, String mapping) {
    	Mapping m = getMapping(context, mapping);
    	return m.getMappedTargetFields();
    }
    
    @TLFunctionInitAnnotation()
    public static final void getMappedTargetFieldsInit(TLFunctionCallContext context){
    	context.setCache(new TLMappingCache(context, 0));
    }
    
    private static class GetMappedTargetFieldsFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			getMappedTargetFieldsInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			int paramCount = context.getParams().length;
			if (paramCount == 1) { // only mapping
				stack.push(getMappedTargetFields(context, stack.popString()));
			} else {
				Integer sourceIdx = null;
				if (paramCount > 2) {
					sourceIdx = stack.popInt();
			    	Objects.requireNonNull(sourceIdx, "Source index is null");
				}
				String sourceField = stack.popString();
				String mapping = stack.popString();
				stack.push(doGetMappedTargetFields(context, mapping, sourceField, sourceIdx));
			}
		} 

    }

	// IS TARGET FIELD MAPPED
	
    @TLFunctionAnnotation("Returns true if at least one source field maps to the specified target field")
    public static final Boolean isTargetFieldMapped(TLFunctionCallContext context, String mapping, String targetField) {
    	Mapping m = getMapping(context, mapping);
    	return m.isTargetFieldMapped(targetField);
    }
    
    @TLFunctionInitAnnotation()
    public static final void isTargetFieldMappedInit(TLFunctionCallContext context){
    	context.setCache(new TLMappingCache(context, 0));
    }
    
    private static class IsTargetFieldMappedFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			isTargetFieldMappedInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			String targetField = stack.popString();
			String mapping = stack.popString();
			stack.push(isTargetFieldMapped(context, mapping, targetField));
		} 

    }

	// IS TARGET FIELD MAPPED
	
    @TLFunctionAnnotation("Returns true if the specified source field is mapped")
    public static final Boolean isSourceFieldMapped(TLFunctionCallContext context, String mapping, String sourceField, Integer sourceIdx) {
    	Mapping m = getMapping(context, mapping);
    	return m.isSourceFieldMapped(sourceField, sourceIdx);
    }
    
    @TLFunctionAnnotation("Returns true if source field with the specified name is mapped")
    public static final Boolean isSourceFieldMapped(TLFunctionCallContext context, String mapping, String sourceField) {
    	return isSourceFieldMapped(context, mapping, sourceField, null);
    }
    
    @TLFunctionInitAnnotation()
    public static final void isSourceFieldMappedInit(TLFunctionCallContext context){
    	context.setCache(new TLMappingCache(context, 0));
    }
    
    private static class IsSourceFieldMappedFunction implements TLFunctionPrototype {

		@Override
		public void init(TLFunctionCallContext context) {
			isSourceFieldMappedInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			Integer sourceIdx = null;
			if (context.getParams().length > 2) {
				sourceIdx = stack.popInt();
			}
			String sourceField = stack.popString();
			String mapping = stack.popString();
			stack.push(isSourceFieldMapped(context, mapping, sourceField, sourceIdx));
		} 

    }
    
	private static Mapping getMapping(TLFunctionCallContext context, String mappingCode) {
		return ((TLMappingCache) context.getCache()).getCachedMapping(context, mappingCode);
	}
    
	private static String getElementName(String inputName, boolean allowOrder) {
		if (inputName.startsWith("$")) {
			String resultName = inputName.substring(1);
			if (allowOrder) {
				Matcher matcher = ORDER_PATTERN.matcher(resultName);
				if (matcher.matches()) {
					resultName = matcher.group(1);
				}
			}
			if (StringUtils.isValidObjectName(resultName)) {
				return resultName;
			}
		}
		throw new IllegalArgumentException("field name \"" + inputName + "\" is not valid.");
	}
    
    private static class MappingElement {
    	public final String source;
    	public final String target;

		public MappingElement(String mapping) {
			String[] split = ASSIGNMENT_PATTERN.split(mapping, 2);

			target = getElementName(split[0], true);
			source = getElementName(split[1], false);
		}
	}
    
    static class Mapping {
    	
    	private MappingElement[][] elements;
    	
    	public Mapping(String code) {
			String[] sources = HASH_PATTERN.split(code, -1);
    		elements = new MappingElement[sources.length][];
    		for (int i = 0; i < sources.length; i++) {
				if (sources[i].trim().length() == 0) { // Ignore empty string
					elements[i] = new MappingElement[0];
					continue;
				}
    			String[] elementsStr = SEMICOLON_PATTERN.split(sources[i]);
    			MappingElement[] elems = new MappingElement[elementsStr.length];
    			elements[i] = elems;
    			
    			for (int j = 0; j < elementsStr.length; j++) {
    				elems[j] = new MappingElement(elementsStr[j]);
    			}
    		}
    	}
    	
		private MappingElement[][] getGroups(Integer sourceIdx) {
    		if (sourceIdx == null) {
    			return elements;
    		}
    		return new MappingElement[][] { elements[sourceIdx] };
    	}
    	
    	/**
    	 * Returns the set of fields from the specified source
    	 * mapped to the specified target field.
    	 * If sourceIdx is null, returns the set of fields from all sources.
    	 * It is not possible to determine the source index,
    	 * should only be used for single source or disjoint sets.
    	 * 
    	 * @param targetField
    	 * @param sourceIdx
    	 * @return
    	 */
    	public List<String> getMappedSourceFields(String targetField, Integer sourceIdx) {
    		MappingElement[][] groups = getGroups(sourceIdx);
    		Set<String> result = new LinkedHashSet<String>();
    		for (MappingElement[] group: groups) {
        		for (MappingElement mappingElement: group) {
        			if (mappingElement.target.equals(targetField)) {
        				result.add(mappingElement.source);
        			}
        		}
    		}
    		return new ArrayList<String>(result);
    	}

		/**
		 * Returns the set of mapped source fields.
		 * @return
		 */
		public List<String> getMappedSourceFields() {
    		Set<String> result = new LinkedHashSet<String>();
    		for (MappingElement[] group: elements) {
    			for (MappingElement mappingElement: group) {
    				result.add(mappingElement.source);
    			}
    		}
    		return new ArrayList<String>(result);
		}

    	/**
    	 * Returns the set of target fields mapped from the specified source field.
    	 * If sourceIdx is null, returns the set of target fields
    	 * mapped from source fields with the specified name from all sources.
    	 * 
    	 * @param targetField
    	 * @param sourceIdx
    	 * @return
    	 */
    	public List<String> getMappedTargetFields(String sourceField, Integer sourceIdx) {
    		MappingElement[][] groups = getGroups(sourceIdx);
    		Set<String> result = new LinkedHashSet<String>();
    		for (MappingElement[] group: groups) {
    			for (MappingElement mappingElement: group) {
    				if (mappingElement.source.equals(sourceField)) {
    					result.add(mappingElement.target);
    				}
    			}
    		}
    		return new ArrayList<String>(result);
    	}

		/**
		 * Returns the set of mapped target fields.
		 * @return
		 */
		public List<String> getMappedTargetFields() {
			Set<String> result = new LinkedHashSet<String>();
    		for (MappingElement[] group: elements) {
    			for (MappingElement mappingElement: group) {
    				result.add(mappingElement.target);
    			}
    		}
    		return new ArrayList<String>(result);
		}

    	public boolean isSourceFieldMapped(String sourceField, Integer sourceIdx) {
    		MappingElement[][] groups = getGroups(sourceIdx);
    		for (MappingElement[] group: groups) {
    			for (MappingElement mappingElement: group) {
    				if (mappingElement.source.equals(sourceField)) {
    					return true;
    				}
    			}
    		}
    		return false;
    	}

    	public boolean isTargetFieldMapped(String targetField) {
    		for (MappingElement[] group: elements) {
    			for (MappingElement mappingElement: group) {
    				if (mappingElement.target.equals(targetField)) {
    					return true;
    				}
    			}
    		}
    		return false;
    	}
    	
    }

}

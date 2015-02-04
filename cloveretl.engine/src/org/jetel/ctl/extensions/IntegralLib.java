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

import java.util.regex.Matcher;

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.data.DataRecord;
import org.jetel.data.NullRecord;

/**
 * These functions have to be part of core engine plugin, since can be invoked direct from
 * CTL executor. For example function copyByName() function is directly used for evaluation
 * of 'record1.* = record2.*' assignment expression.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9.8.2010
 */
public class IntegralLib extends TLFunctionLibrary {
	
    @Override
    public TLFunctionPrototype getExecutable(String functionName) 
    throws IllegalArgumentException {
    	TLFunctionPrototype ret = 
    		"copyByName".equals(functionName) ? new CopyByNameFunction() :
       		"copyByPosition".equals(functionName) ? new CopyByPositionFunction() : 
       		"resetRecord".equals(functionName) ? new ResetRecordFunction() : null; 

    	if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    	
    	return ret;
    }
    
	private static String LIBRARY_NAME = "Record";

	@Override
	public String getName() {
		return LIBRARY_NAME;
	}

	// COPY BY NAME - WARNING - whenever this function will be changed TypeChecker.visit(CLVFAssignment node, Object data)
	// method has to be updated and all related code - this method is directly used for evaluation of 
	// 'record1.* = record.*' assignment expression 
	@TLFunctionInitAnnotation
	public static final void copyByNameInit(TLFunctionCallContext context) {
		TLCopyByNameTransformCache transformCache = new TLCopyByNameTransformCache();
		context.setCache(transformCache);
	}

	@TLFunctionAnnotation("Copies data from the second parameter to the first parameter based on field names. Returns the first argument")
	public static final DataRecord copyByName(TLFunctionCallContext context, DataRecord to, DataRecord from) {
		if (from == null || from.equals(NullRecord.NULL_RECORD)) {
			to.reset();
			return to;
		}
		TLCopyByNameTransformCache transformCache = (TLCopyByNameTransformCache) context.getCache();
		try {
			transformCache.init(from.getMetadata(), to.getMetadata());
			transformCache.transform(from, to);
		} catch (Exception e) {
			throw new TransformLangExecutorRuntimeException("copyByName", e);
		}
		return to;
	}
	
	class CopyByNameFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
			copyByNameInit(context);
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			DataRecord from = stack.popRecord();
			DataRecord to = stack.popRecord();

			copyByName(context, to, from);
		}
	}

	// COPY BY POSITION
	@TLFunctionAnnotation("Copies data from the second parameter to the first parameter based on fields order. Returns the first argument")
	public static final DataRecord copyByPosition(TLFunctionCallContext context, DataRecord to, DataRecord from) {
		to.copyFieldsByPosition(from);
		return to;
	}
	
	class CopyByPositionFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			DataRecord from = stack.popRecord();
			DataRecord to = stack.popRecord();

			copyByPosition(context, to, from);
		}
	}

	// RESET RECORD
	@TLFunctionAnnotation("Resets the given record. All fields are set to null or default value in case non-nullable fields.")
	public static final void resetRecord(TLFunctionCallContext context, DataRecord record) {
		record.reset();
	}
	
	class ResetRecordFunction implements TLFunctionPrototype {
		
		@Override
		public void init(TLFunctionCallContext context) {
		}

		@Override
		public void execute(Stack stack, TLFunctionCallContext context) {
			DataRecord record = stack.popRecord();

			resetRecord(context, record);
		}
	}

	/**
	 * This function should NOT be annotated.
	 * The code is here so that it can be called from the interpreter and compiler
	 * in CLVFComparison.
	 * 
	 * The annotated function is in StringLib.
	 */
	public static final void matchesInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}

	/**
	 * This function should NOT be annotated.
	 * The code is here so that it can be called from the interpreter and compiler
	 * in CLVFComparison.
	 * 
	 * The annotated function is in StringLib.
	 */
	public static final Boolean matches(TLFunctionCallContext context, String input, String pattern) {
		if (input != null){
			Matcher m = ((TLRegexpCache) context.getCache()).getCachedMatcher(context, pattern).reset(input);
			return m.matches();
		}else{
			return false;
		}
	}

	/**
	 * This function should NOT be annotated.
	 * The code is here so that it can be called from the interpreter and compiler
	 * in CLVFComparison.
	 */
	public static final void containsMatchInit(TLFunctionCallContext context) {
		context.setCache(new TLRegexpCache(context, 1));
	}

	/**
	 * This function should NOT be annotated.
	 * The code is here so that it can be called from the interpreter and compiler
	 * in CLVFComparison.
	 */
	public static final Boolean containsMatch(TLFunctionCallContext context, String input, String pattern) {
		Matcher m = ((TLRegexpCache) context.getCache()).getCachedMatcher(context, pattern).reset(input);
		return m.find();
	}
}

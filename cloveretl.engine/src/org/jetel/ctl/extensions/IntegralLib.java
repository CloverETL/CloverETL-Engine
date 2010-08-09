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

import org.jetel.ctl.Stack;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.data.DataRecord;

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
    		"copyByPosition".equals(functionName) ? new CopyByPositionFunction() : null; 

    	if (ret == null) {
    		throw new IllegalArgumentException("Unknown function '" + functionName + "'");
    	}
    	
    	return ret;
    }
    
	private static String LIBRARY_NAME = "Integral";

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
		TLCopyByNameTransformCache transformCache = (TLCopyByNameTransformCache) context.getCache();
		try {
			transformCache.init(from.getMetadata(), to.getMetadata());
			transformCache.transform(from, to);
		} catch (Exception e) {
			throw new TransformLangExecutorRuntimeException("copyByName - " + e.getMessage(), e);
		}
		return to;
	}
	
	class CopyByNameFunction implements TLFunctionPrototype {
		
		public void init(TLFunctionCallContext context) {
			copyByNameInit(context);
		}

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
		
		public void init(TLFunctionCallContext context) {
		}

		public void execute(Stack stack, TLFunctionCallContext context) {
			DataRecord from = stack.popRecord();
			DataRecord to = stack.popRecord();

			copyByPosition(context, to, from);
		}
	}

}

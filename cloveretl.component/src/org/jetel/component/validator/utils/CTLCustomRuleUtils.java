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
package org.jetel.component.validator.utils;

import java.util.ArrayList;
import java.util.List;

import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.NavigatingVisitor;
import org.jetel.ctl.TLCompiler;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLType;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 18.4.2013
 */
public class CTLCustomRuleUtils {
    private static class CtlFunctionsFinder extends NavigatingVisitor {
    	private final org.jetel.ctl.ASTnode.Node ast;
    	public DataRecordMetadata[] inputMetadata;
    	public DataRecordMetadata[] outputMetadata;
    	public List<Function> functions = new ArrayList<Function>();
    	
    	public CtlFunctionsFinder(org.jetel.ctl.ASTnode.Node ast, DataRecordMetadata [] inputMetadata, DataRecordMetadata[] outputMetadata) {
    		this.ast = ast;
    		this.inputMetadata = inputMetadata;
    		this.outputMetadata = outputMetadata;
    	}
    	
    	/**
    	 * Scans AST tree for functions which are returned for further usage.
    	 * 
    	 * @return	Set of functions found in given source
    	 */
    	public List<Function> visit() {
    		functions.clear();
    		ast.jjtAccept(this, null);
    		return functions;
    	}
    	
    	@Override
    	public Object visit(CLVFFunctionDeclaration node, Object data) {
    		functions.add(new Function(node.name, node.getFormalParameters()));
    		return super.visit(node, data);
    	}
    }

    public static List<Function> findFunctions(TransformationGraph graph, DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String code) {
    	TLCompiler compiler = new TLCompiler(graph,inMeta,outMeta);
    	List<ErrorMessage> msgs = compiler.validate(code);
    	if (compiler.errorCount() > 0) {
    		StringBuilder messagesStringBuilder = new StringBuilder();
    		for (ErrorMessage msg : msgs) {
    			messagesStringBuilder.append(msg.getErrorMessage());
    			messagesStringBuilder.append("; ");
    		}
    		throw new JetelRuntimeException(messagesStringBuilder.toString());
    	}

    	
    	CtlFunctionsFinder ctlFunctionsFinder = new CtlFunctionsFinder(compiler.getStart(), inMeta, outMeta);
    	return ctlFunctionsFinder.visit();
    }
    
    public static class Function {
    	private String name;
    	private TLType[] parametersType;
    	public Function(String name, TLType[] parameters) {
    		this.name = name;
    		parametersType = parameters;
    	}
    	
		public String getName() {
			return name;
		}
		
		public TLType[] getParametersType() {
			return parametersType;
		}
    }
}
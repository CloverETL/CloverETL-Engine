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
package org.jetel.util;

import static org.jetel.ctl.TransformLangParserTreeConstants.JJTFUNCTIONDECLARATION;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTIMPORTSOURCE;

import java.util.ArrayList;
import java.util.List;

import org.jetel.component.RecordTransformTL;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.NavigatingVisitor;
import org.jetel.ctl.TLCompiler;
import org.jetel.ctl.ASTnode.CLVFFieldAccessExpression;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * @author Pavel Simecek (pavel.simecek@javlin.eu)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.6.2012
 */
public class CTLTransformUtils {
    private static class CtlAssignmentFinder extends NavigatingVisitor {
    	private final org.jetel.ctl.ASTnode.Node ast;
    	public DataRecordMetadata [] inputMetadata;
    	public List<DataFieldMetadata> assignedFields = new ArrayList<DataFieldMetadata>();
    	
    	/**
    	 * Allocates verifier which will verify that the <code>functionName</code>
    	 * contains only simple mappings
    	 * 
    	 * @param functionName	function to validate
    	 */
    	public CtlAssignmentFinder(org.jetel.ctl.ASTnode.Node ast, DataRecordMetadata [] inputMetadata) {
    		this.ast = ast;
    		this.inputMetadata = inputMetadata;
    	}
    	
    	/**
    	 * Scans AST tree for function and checks it only contains direct mappings
    	 * (i.e. assignments where LHS is an output field reference)
    	 * 
    	 * @return	true if function is simple, false otherwise
    	 */
    	public List<DataFieldMetadata> visit() {
    		assignedFields.clear();
    		ast.jjtAccept(this, null);
    		return assignedFields;
    	}
    	
    	@Override
    	public Object visit(org.jetel.ctl.ASTnode.CLVFStart node, Object data) {
    		// functions can only be declared in start or import nodes
    		for (int i=0; i<node.jjtGetNumChildren(); i++) {
    			final SimpleNode child = (SimpleNode)node.jjtGetChild(i); 
    			final int id = child.getId();
    			switch (id) {
    			case JJTIMPORTSOURCE:
    				// scan imports
    				child.jjtAccept(this, data);
    				break;
    			case JJTFUNCTIONDECLARATION:
    				if (((org.jetel.ctl.ASTnode.CLVFFunctionDeclaration)child).getName().equals(RecordTransformTL.TRANSFORM_FUNCTION_NAME)) {
    					// scan statements in function body 
    					return child.jjtGetChild(2).jjtAccept(this, data);
    				}
    				break;
    			
    			}
    		}
    		
    		return false;
    	}

    	@Override
		public Object visit(CLVFFieldAccessExpression fieldAccess, Object data) {
			if (fieldAccess.getRecordId()!=null && fieldAccess.getRecordId()>=0 && fieldAccess.getRecordId()<inputMetadata.length && !StringUtils.isEmpty(fieldAccess.getFieldName())) {
				DataRecordMetadata inputRecordMetadata = inputMetadata[fieldAccess.getRecordId()] ;
				if (inputRecordMetadata != null) {
					DataFieldMetadata assignedFieldMetadata = inputRecordMetadata.getField(fieldAccess.getFieldName());
					if (assignedFieldMetadata!=null) {
	    				assignedFields.add(assignedFieldMetadata);
					}
				}
			}

			return super.visit(fieldAccess, data);
		}
    }

    static public List<DataFieldMetadata> findUsedInputFields(TransformationGraph graph, DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String code) {
    	TLCompiler compiler = new TLCompiler(graph,inMeta,outMeta);
    	List<ErrorMessage> msgs = compiler.validate(code);
    	if (compiler.errorCount() > 0) {
    		StringBuilder messagesStringBuilder = new StringBuilder();
    		for (ErrorMessage msg : msgs) {
    			messagesStringBuilder.append(msg.getErrorMessage());
    			messagesStringBuilder.append("\n");
    		}
    		throw new JetelRuntimeException("Failed to find assigned fields:\n" + messagesStringBuilder.toString());
    	}

    	
    	CtlAssignmentFinder ctlAssignmentFinder = new CtlAssignmentFinder(compiler.getStart(), inMeta);
    	return ctlAssignmentFinder.visit();
    }
	
	
}

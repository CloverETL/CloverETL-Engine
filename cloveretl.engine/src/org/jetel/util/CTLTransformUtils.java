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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jetel.component.RecordTransformTL;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.NavigatingVisitor;
import org.jetel.ctl.TLCompiler;
import org.jetel.ctl.TransformLangParserTreeConstants;
import org.jetel.ctl.ASTnode.CLVFAssignment;
import org.jetel.ctl.ASTnode.CLVFFieldAccessExpression;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/**
 * The class has the only public method findUsedInputFields that is used to find all fields
 * used on a right side of assignment in a given CTL transformation.
 * 
 * Limitations:
 *   - Only a list of fields is returned - if a field is shared by two metadata it is not clear from which metadata the field comes from 
 *   - this utility works only for really very simple transformations,
 *   	actually it works only for transformation created by drag and drop events in transform dialog
 *   
 * @author Pavel Simecek (pavel.simecek@javlin.eu)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.6.2012
 */
public class CTLTransformUtils {
    private static class CtlAssignmentFinder extends NavigatingVisitor {
    	private final org.jetel.ctl.ASTnode.Node ast;
    	public DataRecordMetadata[] inputMetadata;
    	public DataRecordMetadata[] outputMetadata;
    	public Set<Field> assignedFields = new HashSet<Field>();
    	
    	/**
    	 * Allocates verifier which will verify that the <code>functionName</code>
    	 * contains only simple mappings
    	 * 
    	 * @param functionName	function to validate
    	 */
    	public CtlAssignmentFinder(org.jetel.ctl.ASTnode.Node ast, DataRecordMetadata [] inputMetadata, DataRecordMetadata[] outputMetadata) {
    		this.ast = ast;
    		this.inputMetadata = inputMetadata;
    		this.outputMetadata = outputMetadata;
    	}
    	
    	/**
    	 * Scans AST tree for function and checks it only contains direct mappings
    	 * (i.e. assignments where LHS is an output field reference)
    	 * 
    	 * @return	true if function is simple, false otherwise
    	 */
    	public Set<Field> visit() {
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
    		DataRecordMetadata[] metaList = fieldAccess.isOutput() ? outputMetadata : inputMetadata;
			if (fieldAccess.getRecordId() != null
					&& fieldAccess.getRecordId() >= 0
					&& fieldAccess.getRecordId() < metaList.length
					&& !StringUtils.isEmpty(fieldAccess.getFieldName())) {
				DataRecordMetadata recordMetadata = metaList[fieldAccess.getRecordId()] ;
				if (recordMetadata != null) {
					DataFieldMetadata assignedFieldMetadata = recordMetadata.getField(fieldAccess.getFieldName());
					if (assignedFieldMetadata != null) {
	    				assignedFields.add(new Field(assignedFieldMetadata.getName(), fieldAccess.getRecordId(), fieldAccess.isOutput()));
					}
				}
			}

			return super.visit(fieldAccess, data);
		}
    	
    	/**
    	 * This visitor method detects assignment with following shape:<br>
    	 * $out.0.* = $in.0.*
    	 */
    	@Override
    	public Object visit(CLVFAssignment assignment, Object data) {
    		SimpleNode lhs = (SimpleNode) assignment.jjtGetChild(0);
    		SimpleNode rhs = (SimpleNode) assignment.jjtGetChild(1);

    		if (lhs.getId() == TransformLangParserTreeConstants.JJTFIELDACCESSEXPRESSION &&
    				rhs.getId() == TransformLangParserTreeConstants.JJTFIELDACCESSEXPRESSION) {
    			final CLVFFieldAccessExpression leftNode = (CLVFFieldAccessExpression) lhs;
    			final CLVFFieldAccessExpression rightNode = (CLVFFieldAccessExpression) rhs;
    			DataRecordMetadata[] leftMetaList = leftNode.isOutput() ? outputMetadata : inputMetadata;
    			DataRecordMetadata[] rightMetaList = rightNode.isOutput() ? outputMetadata : inputMetadata;
    			
    			if (leftNode.isWildcard() && rightNode.isWildcard()) {
        			final DataRecordMetadata leftRecordMetadata = leftMetaList[leftNode.getRecordId()];
        			final DataRecordMetadata rightRecordMetadata = rightMetaList[rightNode.getRecordId()];
    				for (DataFieldMetadata rightField : rightRecordMetadata) {
    					DataFieldMetadata leftField;
    					if ((leftField = leftRecordMetadata.getField(rightField.getName())) != null) {
    						assignedFields.add(new Field(rightField.getName(), rightNode.getRecordId(), rightNode.isOutput()));
    						assignedFields.add(new Field(leftField.getName(), leftNode.getRecordId(), leftNode.isOutput()));
    					}
    				}
    			}

    		}
    		
    		return super.visit(assignment, data);
    	}
    }

    public static Set<Field> findUsedFields(TransformationGraph graph, DataRecordMetadata[] inMeta,
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

    	
    	CtlAssignmentFinder ctlAssignmentFinder = new CtlAssignmentFinder(compiler.getStart(), inMeta, outMeta);
    	return ctlAssignmentFinder.visit();
    }
	
    private static Set<Field> findUsedOutputFields(TransformationGraph graph, DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String code, boolean output) {
    	Set<Field> result = findUsedFields(graph, inMeta, outMeta, code);
    	for (Iterator<Field> it = result.iterator(); it.hasNext(); ) {
    		Field f = it.next();
    		if (f.output != output) {
    			it.remove();
    		}
    	}
    	return result;
    }
    
    public static Set<Field> findUsedOutputFields(TransformationGraph graph, DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String code) {
    	return findUsedOutputFields(graph, inMeta, outMeta, code, true);
    }
    
    public static Set<Field> findUsedInputFields(TransformationGraph graph, DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String code) {
    	return findUsedOutputFields(graph, inMeta, outMeta, code, false);
    }
    
    /**
     * A field is determined by:
     * <ul>
     * 	<li>input/output</li>
     * 	<li>record ID</li>
     *	<li>field name</li>
     * </ul>
     */
	public static class Field {
		public final String name;
		public final int recordId;
		public final boolean output;
		
		public Field(String name, int recordId, boolean output) {
			this.name = name;
			this.recordId = recordId;
			this.output = output;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result + (output ? 1231 : 1237);
			result = prime * result + recordId;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Field other = (Field) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (output != other.output)
				return false;
			if (recordId != other.recordId)
				return false;
			return true;
		}

		@Override
		public String toString() {
			return (output ? "$out." : "$in.") + recordId + "." + name;
		}
	}
}

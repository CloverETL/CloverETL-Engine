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
package org.jetel.ctl;

import static org.jetel.ctl.TransformLangParserTreeConstants.JJTASSIGNMENT;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTFIELDACCESSEXPRESSION;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTFUNCTIONDECLARATION;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTIMPORTSOURCE;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTRETURNSTATEMENT;

import java.util.List;

import org.jetel.ctl.ASTnode.CLVFBlock;
import org.jetel.ctl.ASTnode.CLVFFieldAccessExpression;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.data.Defaults;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

public final class TLUtils {

	public static DataFieldMetadata getFieldMetadata(DataRecordMetadata[] recordMetadata, int recordNo, int fieldNo) {
		if (recordNo >= recordMetadata.length) {
			throw new IllegalArgumentException("Record [" + recordNo + "] does not exist");
		}
		DataRecordMetadata record = recordMetadata[recordNo];
		if (record == null) {
			throw new IllegalArgumentException("Metadata for record [ " + recordNo + "] null (not assigned?)");
		}
		
		return record.getField(fieldNo);
	}

	public static String operatorToString(int operator) {
		switch (operator) {
			case TransformLangParserConstants.EQUAL:
				return "==";
			case TransformLangParserConstants.NON_EQUAL:
				return "!=";
			case TransformLangParserConstants.LESS_THAN:
				return "<";
			case TransformLangParserConstants.LESS_THAN_EQUAL:
				return "<=";
			case TransformLangParserConstants.GREATER_THAN:
				return ">";
			case TransformLangParserConstants.GREATER_THAN_EQUAL:
				return ">=";

			default:
				// the operator is unknown
				return null;
		}
	}

	/**
	 * Compares two given metadata objects.
	 * Metadata objects are considered as equal if have same number of fields and fields are equal
	 * (see {@link #equals(DataFieldMetadata, DataFieldMetadata)}).
	 * @param metadata1
	 * @param metadata2
	 * @return <code>true</code> if metadata objects are considered as equal, <code>false</code> otherwise
	 */
	public static boolean equals(DataRecordMetadata metadata1, DataRecordMetadata metadata2) {
		if (metadata1 == null || metadata2 == null) {
			return false;
		}

		if (metadata1 == metadata2) {
			return true;
		}

		if (metadata1.getNumFields() != metadata2.getNumFields()) {
			return false;
		}

		for (int i = 0; i < metadata1.getNumFields(); i++) {
			if (!equals(metadata1.getField(i), metadata2.getField(i))) {
				return false;
			}
		}

		return true;
	}
	
	/**
	 * Compares two given metadata fields.
	 * Metadata fields are considered as equal if have same name and type.
	 * @param field1
	 * @param field2
	 * @return <code>true</code> if metadata fields are considered as equal, <code>false</code> otherwise
	 */
	public static boolean equals(DataFieldMetadata field1, DataFieldMetadata field2) {
		return equals(field1, field2, false, null);
	}

	/**
	 * Compares two given metadata fields.
	 * Stores warnings in the provided list.
	 * 
	 * Metadata fields are considered as equal if have same name (ignore case) and type.
	 * @param field1
	 * @param field2
	 * @param warnings list of warnings
	 * @return <code>true</code> if metadata fields are considered as equal, <code>false</code> otherwise
	 */
	public static boolean equalsIgnoreCase(DataFieldMetadata field1, DataFieldMetadata field2, List<String> warnings) {
		return equals(field1, field2, true, warnings);
	}
	
	/**
	 * Compares two given metadata fields.
	 * Metadata fields are considered as equal if have same name (ignore case) and type.
	 * @param field1
	 * @param field2
	 * @return <code>true</code> if metadata fields are considered as equal, <code>false</code> otherwise
	 */
	public static boolean equalsIgnoreCase(DataFieldMetadata field1, DataFieldMetadata field2) {
		return equals(field1, field2, true, null);
	}

	private static boolean equals(DataFieldMetadata field1, DataFieldMetadata field2, boolean ignoreCase, List<String> warnings) {
		
		if (field1 == null || field2 == null) {
			return false;
		}
		
		//field names have to be equal
		if (!field1.getName().equalsIgnoreCase(field2.getName()) && ignoreCase) {
			return false;
		}
		if (!field1.getName().equals(field2.getName()) && !ignoreCase) {
			return false;
		}
		
		//field types have to be equal
		if (! (field1.getDataType() == field2.getDataType())) {
			return false;
		}
		
		//container types have to be equal
		if (field1.getContainerType() != field2.getContainerType()) {
			return false;
		}
		
		if (field1.getDataType() == DataFieldType.DECIMAL) {
			
			if (!StringUtils.equalsWithNulls(field1.getProperty(DataFieldMetadata.LENGTH_ATTR),
					field2.getProperty(DataFieldMetadata.LENGTH_ATTR))
					|| !StringUtils.equalsWithNulls(field1.getProperty(DataFieldMetadata.SCALE_ATTR),
					field2.getProperty(DataFieldMetadata.SCALE_ATTR))) {
				if (warnings != null) {
					StringBuilder sb = new StringBuilder();
					sb.append("Incompatible types - ");
					sb.append(field1.getName()).append(": ").append(field1.getDataType()).append('(').append(field1.getProperty(DataFieldMetadata.LENGTH_ATTR)).append(',').append(field1.getProperty(DataFieldMetadata.SCALE_ATTR)).append(')');
					sb.append(" and ");
					sb.append(field2.getName()).append(": ").append(field2.getDataType()).append('(').append(field2.getProperty(DataFieldMetadata.LENGTH_ATTR)).append(',').append(field2.getProperty(DataFieldMetadata.SCALE_ATTR)).append(')');
					warnings.add(sb.toString());
				}
				return false;
			}
		}
		
		return true;
	}
	
    /**
     * Verifier class checking if a CTL function only contains direct mappings.
     * Direct mapping is an assignment statement where left hand side contains an output field reference.
     * 
     * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
     */
    private static class SimpleTransformVerifier extends NavigatingVisitor {
    	private final String functionName;
    	private final org.jetel.ctl.ASTnode.Node ast;
    	
    	/**
    	 * Allocates verifier which will verify that the <code>functionName</code>
    	 * contains only simple mappings
    	 * 
    	 * @param functionName	function to validate
    	 */
    	public SimpleTransformVerifier(String functionName, org.jetel.ctl.ASTnode.Node ast) {
    		this.functionName = functionName;
    		this.ast = ast;
    		
    	}
    	
    	/**
    	 * Scans AST tree for function and checks it only contains direct mappings
    	 * (i.e. assignments where LHS is an output field reference)
    	 * 
    	 * @return	true if function is simple, false otherwise
    	 */
    	public boolean check() {
    		return (Boolean)ast.jjtAccept(this, null);
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
    				if (((org.jetel.ctl.ASTnode.CLVFFunctionDeclaration)child).getName().equals(functionName)) {
    					// scan statements in function body 
    					return child.jjtGetChild(2).jjtAccept(this, data);
    				}
    				break;
    			
    			}
    		}
    		
    		return false;
    	}

    	@Override
    	public Object visit(CLVFBlock node, Object data) {
    		// we must have come here as the block is 'transform' function body
    		for (int i=0; i<node.jjtGetNumChildren(); i++) {
    			final SimpleNode child = (SimpleNode)node.jjtGetChild(i);

    			// statement must be an assignment and a direct mapping into output field
    			if (child.getId() != JJTASSIGNMENT && child.getId() != JJTRETURNSTATEMENT) {
    				// not an assignment - fail quickly
    				return false;
    			}
    			
    			if (child.getId() != JJTRETURNSTATEMENT) {
	    			// check if direct mapping
	    			final SimpleNode lhs = (SimpleNode)child.jjtGetChild(0);
	    			if (lhs.getId() != JJTFIELDACCESSEXPRESSION) {
	    				// not a mapping
	    				return false;
	    			}
	    			if (!((CLVFFieldAccessExpression) lhs).isOutput()) {
	    				// lhs must be an output field
	    				return false;
	    			}
    			}
    		}
    		
    		// all statements are direct mappings
    		return true;
    	}
    }

    public static boolean isSimpleFunction(TransformationGraph graph, DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String code, String functionName, String charset) {
    	
    	if (charset == null) {
    		charset = Defaults.DEFAULT_SOURCE_CODE_CHARSET;
    	}
    	TLCompiler compiler = new TLCompiler(graph,inMeta,outMeta,charset);
    	List<ErrorMessage> msgs = compiler.validate(code);
    	if (compiler.errorCount() > 0) {
    		for (ErrorMessage msg : msgs) {
    			System.out.println(msg);
    		}
    		System.out.println("CTL code compilation finished with " + compiler.errorCount() + " errors");
    		return false;
    	}


    	final SimpleTransformVerifier verifier = new SimpleTransformVerifier(functionName,compiler.getStart());
    	return verifier.check();
    }
	
	private TLUtils() {
		// not available
	}

}

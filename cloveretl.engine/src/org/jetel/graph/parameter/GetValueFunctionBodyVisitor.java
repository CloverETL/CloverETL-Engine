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
package org.jetel.graph.parameter;

import org.jetel.ctl.CodePieceExtractor.PositionedCodeVisitor;
import org.jetel.ctl.CodePieceExtractor.PositionedCodeVisitorFactory;
import org.jetel.ctl.CodePieceExtractor.SyntacticPositionedString;
import org.jetel.ctl.ASTnode.CLVFBlock;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.Node;

/**
 * AST visitor that extracts parameter's getValue() function body code.
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12. 5. 2014
 */
public class GetValueFunctionBodyVisitor extends PositionedCodeVisitor {
	
	private final static PositionedCodeVisitorFactory<GetValueFunctionBodyVisitor> factory = new PositionedCodeVisitorFactory<GetValueFunctionBodyVisitor>() {
		@Override
		public GetValueFunctionBodyVisitor getPositionedCodeVisitor(
				SyntacticPositionedString positionedSourceCode) {
			return new GetValueFunctionBodyVisitor(positionedSourceCode);
		}
	};
	
	public static PositionedCodeVisitorFactory<GetValueFunctionBodyVisitor> getFactory() {
		return factory;
	}
	
	private String code;

	private GetValueFunctionBodyVisitor(SyntacticPositionedString positionedSourceCode) {
		super(positionedSourceCode);
	}
	
	@Override
	public Object visit(CLVFFunctionDeclaration node, Object data) {
		if (!node.getName().equals(GraphParameterValueFunction.GET_PARAMETER_VALUE_FUNCTION_NAME)) {
			return super.visit(node, data);
		}
		if (node.jjtGetNumChildren() < 3) {
			return super.visit(node, data);
		}
		Node bodyObject = node.jjtGetChild(2);
		if (bodyObject instanceof CLVFBlock) {
			CLVFBlock block = (CLVFBlock) bodyObject;
			code = getCodePiece(block);
		}
		return super.visit(node, data);
	}
	
	/**
	 * Returns the code of getValue() function body. That is, the text between "{" and "}" braces, including these braces.
	 */
	public String getCode() {
		return code;
	}
}

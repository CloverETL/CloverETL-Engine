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

import static org.jetel.ctl.TransformLangParserTreeConstants.JJTFUNCTIONDECLARATION;

import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.ASTnode.CLVFStart;
import org.jetel.ctl.ASTnode.SimpleNode;

/**
 * Function declaration scanner.
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 */
public class FindFunctionVisitor extends NavigatingVisitor {

	/**
	 * Function name to search for
	 */
	private String functionName;

	/**
	 * AST to scan
	 */
	private CLVFStart ast;
	
	public FindFunctionVisitor(CLVFStart ast) {
		this.ast = ast;
	}
	
	/**
	 * Scans AST for function with given name. If more declarations exist, only the 
	 * first one is returned. 
	 * 
	 * @param functionName	Functions to scan for
	 * @return	Corresponding function declaration, or <code>null</code> when not found.
	 */
	public CLVFFunctionDeclaration getFunction(String functionName) {
		this.functionName = functionName;
		return (CLVFFunctionDeclaration) ast.jjtAccept(this, null);
	}
	
	@Override
	public Object visit(CLVFStart node, Object data) {
		for (int i=0; i<node.jjtGetNumChildren(); i++) {
			if (((SimpleNode)node.jjtGetChild(i)).getId() == JJTFUNCTIONDECLARATION) {
				CLVFFunctionDeclaration d = (CLVFFunctionDeclaration)node.jjtGetChild(i);
				if (functionName.equals(d.getName())) {
					return d;
				}
			}
		}
		
		return null;
	}
	
}

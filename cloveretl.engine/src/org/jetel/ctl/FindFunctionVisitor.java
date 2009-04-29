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

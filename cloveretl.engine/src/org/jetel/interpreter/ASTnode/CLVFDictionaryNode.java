package org.jetel.interpreter.ASTnode;

import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.TransformLangParserVisitor;

public class CLVFDictionaryNode extends SimpleNode {

	public static final int OP_READ = 0;
	public static final int OP_WRITE = 1;
	public static final int OP_DELETE = 2;
	
	public int operation;
	
	
	
	public CLVFDictionaryNode(int id) {
		super(id);
	}

	public CLVFDictionaryNode(TransformLangParser p, int id) {
		super(p, id);
	}

	/** Accept the visitor. * */
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}

	public void setOperation(int operation) {
		this.operation = operation;
	}

}

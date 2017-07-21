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
package org.jetel.ctl.ASTnode;
import org.jetel.ctl.ExpParser;
import org.jetel.ctl.SyntacticPosition;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.TransformLangParserTreeConstants;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.TLType;

public abstract class SimpleNode implements Node {
	protected Node parent;
	protected Node[] children;
	protected int id;
	protected ExpParser parser;
	public String sourceFilename;
	private SyntacticPosition begin;
	private SyntacticPosition end;
	private TLType type;
	
	
	public SimpleNode(int i) {
		id = i;
	}

	public SimpleNode(ExpParser p, int i) {
		this(i);
		parser = p;
	}
	
	// copy constructor
	protected SimpleNode(SimpleNode original) {
		this(original.parser,original.id);
		this.parent = original.parent;
		this.sourceFilename = original.sourceFilename;
		this.begin = original.begin;
		this.end = original.end;
		this.type = original.type;
		
		if (original.children != null) {
			this.children = new Node[original.children.length];
			for (int i = 0; i<children.length; i++) {
				children[i] = ((SimpleNode)original.children[i]).duplicate();
			}
		} else {
			this.children = null;
		}
	}

	@Override
	public void jjtOpen() {
	}

	@Override
	public void jjtClose() {
	}

	@Override
	public void jjtSetParent(Node n) {
		parent = n;
	}

	@Override
	public Node jjtGetParent() {
		return parent;
	}

	@Override
	public void jjtAddChild(Node n, int i) {
		if (children == null) {
			children = new Node[i + 1];
		} else if (i >= children.length) {
			Node c[] = new Node[i + 1];
			System.arraycopy(children, 0, c, 0, children.length);
			children = c;
		}
		children[i] = n;
	}

	@Override
	public Node jjtGetChild(int i) {
		return children[i];
	}

	@Override
	public int jjtGetNumChildren() {
		return (children == null) ? 0 : children.length;
	}

	@Override
	public Node removeChild(int i) {
		if (children == null || i >= children.length) {
			return null;
		}
		if (children.length-1 == 0) {
			Node ret = children[0];
			children = null;
			return ret;
		}
		
		Node[] c = new Node[children.length-1];

		if (i==0) {
			Node ret = children[0];
			System.arraycopy(children, 1, c, 0, children.length-1);
			children = c;
			return ret;
		}

		Node ret = children[i];
		System.arraycopy(children, 0, c, 0, i); // copy nodes 1..i
		System.arraycopy(children,i+1,c,i,c.length-i); // copy nodes i+1..n
		children = c;
		return ret;
	}
	
	
	/**
	 * Inserts child to the given position shifting all siblings to the right
	 * (as opposed to {@link #jjtAddChild(Node, int)} which replaces the node on postition i)
	 *
	 * On empty array or position >= children.length replicates {@link #jjtAddChild(Node, int)} behavior. 
	 * 
	 * 
	 * @param n		Node to insert
	 * @param i		position
	 */
	public void insertChild(Node n, int i) {
		if (children == null || i>= children.length) {
			jjtAddChild(n, i);
			return;
		}
		
		Node[] ret = new Node[children.length+1];
		System.arraycopy(children,0,ret,0,i);
		ret[i] = n;
		System.arraycopy(children,i,ret,i+1,children.length-i);
		children = ret;
	}
	
	
	/**
	 * @param child		node to find
	 * @return	index of child among this node's children or -1 if not found
	 */
	public int indexOf(Node child) {
		if (children == null) {
			return -1;
		}
		
		for (int i=0; i<children.length; i++) {
			if (children[i] == child) {
				return i;
			}
		}
		
		return -1;
	}
	
	/** Accept the visitor. * */
	@Override
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		try {
			return visitor.visit(this, data);
		} catch (TransformLangExecutorRuntimeException e) {
			if (e.getNode() == null) {
				e.setNode(this);
			}
			throw e;
		} catch (RuntimeException e) {
			throw new TransformLangExecutorRuntimeException(this, null, e);
		}
	}

	/** Accept the visitor. * */
	public Object childrenAccept(TransformLangParserVisitor visitor, Object data) {
		if (children != null) {
			for (int i = 0; i < children.length; ++i) {
				children[i].jjtAccept(visitor, data);
			}
		}
		return data;
	}

	public boolean jjtHasChildren() {
		return (children != null && children.length > 0) ? true : false;
	}

	/*
	 * You can override these two methods in subclasses of SimpleNode to customize the way the node appears when the
	 * tree is dumped. If your output uses more than one line you should override toString(String), otherwise overriding
	 * toString() is probably all you need to do.
	 */

	@Override
	public String toString() {
		return TransformLangParserTreeConstants.jjtNodeName[id]; 
	}

	public String toString(String prefix) {
		return prefix + toString();
	}

	/*
	 * Override this method if you want to customize how the node dumps out its children.
	 */

	public void dump(String prefix) {
		System.out.println(toString(prefix));
		if (children != null) {
			for (int i = 0; i < children.length; ++i) {
				SimpleNode n = (SimpleNode) children[i];
				if (n != null) {
					n.dump(prefix + " ");
				}
			}
		}
	}

	/*
	 * implicitly call or childern with init(), if there is a need to initialize node, then the node should implement
	 * its own init() method (non-Javadoc)
	 * 
	 * @see org.jetel.interpreter.Node#init()
	 */

	@Override
	public void init() {
		int i, k = jjtGetNumChildren();

		for (i = 0; i < k; i++)
			jjtGetChild(i).init();
	}

	public void begin(int line, int column) {
		this.begin = new SyntacticPosition(line,column);
	}
	
	public int getLine() {
		return getBegin().getLine();
	}
	
	public int getColumn() {
		return getBegin().getColumn();
	}
	
	public SyntacticPosition getBegin() {
		if (begin == null) {
			if (jjtHasChildren()) {
				begin = ((SimpleNode)jjtGetChild(0)).getBegin();
			}
		}
		// this is a Start node without children
		if (begin == null) {
			begin = new SyntacticPosition(1,1);
		}
		return begin;
	}
	
	public void end(int line, int column) {
		this.end = new SyntacticPosition(line,column);
	}
	
	public SyntacticPosition getEnd() {
		if (end == null) {
			if (jjtHasChildren()) {
				end = ((SimpleNode)jjtGetChild(jjtGetNumChildren()-1)).getEnd();
			}
		}

		// this is a Start symbol without children
		if (end == null) {
			end = new SyntacticPosition(1,1);
		}
		return end;
	}
	
	/**
	 * @param filename
	 *            name of file from which this node was created/imported
	 */
	public void setSourceFilename(String filename) {
		this.sourceFilename = filename;
	}

	/**
	 * @return name of file from which this node was created/imported
	 */
	public String getSourceFilename() {
		if (this.sourceFilename != null) {
			return this.sourceFilename;
		} else if (jjtGetParent() != null) {
			return ((SimpleNode) jjtGetParent()).getSourceFilename();
		} else {
			return null;
		}
	}
	
	/**
	 * Return id of this node. This corresponds with TransformLangParserTreeConstants
	 * @return
	 */
	public int getId() {
		return id;
	}
	
	public void setType(TLType type) {
		this.type = type;
	}
	
	public TLType getType() {
		return type;
	}

	@Override
	public void setChildren(Node[] children) {
		this.children = children;
	}
	
	public ExpParser getParser() {
		return parser;
	}
	
	/**
	 *
	 * Duplicates nodes within AST tree. Please be aware that internal links in nodes
	 * are not copied. (i.e. when a resolved identifier node pointing to relevant variable declaration
	 * will be duplicated, it will still point to the same variable. Thus when we copy a variable
	 * as well the link will happen to be invalid!)
	 * 
	 * @return copy of subtree rooted in this node 
	 */
	public abstract SimpleNode duplicate();	
	
}

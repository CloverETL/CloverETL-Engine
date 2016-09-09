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

import java.io.PrintStream;
import java.io.PrintWriter;

import org.jetel.ctl.ExpParser;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.TransformLangParserVisitor;

public class CLVFImportSource extends SimpleNode {

	String sourceToImport;

	public CLVFImportSource(int id) {
		super(id);
	}

	public CLVFImportSource(ExpParser p, int id) {
		super(p, id);
	}

	public CLVFImportSource(CLVFImportSource node) {
		super(node);
	}

	/** Accept the visitor. This method implementation is identical in all SimpleNode descendants. */
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

	public void setSourceToImport(String src) {
		this.sourceToImport = src;
	}

	public String getSourceToImport() {
		return this.sourceToImport;
	}

	@Override
	public SimpleNode duplicate() {
		return new CLVFImportSource(this);
	}
	
	@Override
	public void dump(PrintStream out, String prefix) {
		out.println(String.format("%02d:%s:%s",getLine(),sourceToImport,toString(prefix)));
		if (children != null) {
			for (int i = 0; i < children.length; ++i) {
				SimpleNode n = (SimpleNode) children[i];
				if (n != null) {
					n.dump(out,prefix + " ");
				}
			}
		}
	}

	@Override
	public void dump(PrintWriter out, String prefix) {
		out.format("%02d:%s:%s%n",getLine(),sourceToImport,toString(prefix));
		if (children != null) {
			for (int i = 0; i < children.length; ++i) {
				SimpleNode n = (SimpleNode) children[i];
				if (n != null) {
					n.dump(out,prefix + " ");
				}
			}
		}
	}
}

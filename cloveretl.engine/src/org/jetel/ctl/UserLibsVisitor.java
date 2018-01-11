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

import java.util.ArrayList;
import java.util.List;

import org.jetel.ctl.ASTnode.CLVFLoadClass;
import org.jetel.ctl.ASTnode.CLVFStart;

/**
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11. 1. 2018
 */
public class UserLibsVisitor extends NavigatingVisitor {
	
	public List<String> find(CLVFStart ast) {
		List<String> userLibs = new ArrayList<>();
		visit(ast, userLibs);
		return userLibs;
	}

	@Override
	public Object visit(CLVFLoadClass node, Object data) {
		((List<String>) data).add(node.getClassName());
		return super.visit(node, data);
	}
	
}

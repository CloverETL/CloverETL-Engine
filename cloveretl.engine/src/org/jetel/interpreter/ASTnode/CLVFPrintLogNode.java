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
package org.jetel.interpreter.ASTnode;

import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.TransformLangParserVisitor;

public class CLVFPrintLogNode extends SimpleNode {
    
    public int level;
    
  public CLVFPrintLogNode(int id) {
    super(id);
  }

  public CLVFPrintLogNode(TransformLangParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  public boolean setLevel(String levelStr){
		switch (levelStr.charAt(0)) {
		case 't':
		case 'T':
			level = 1;
			break;
		case 'd':
		case 'D':
			level = 1;
			break;
		case 'i':
		case 'I':
			level = 2;
			break;
		case 'w':
		case 'W':
			level = 3;
			break;
		case 'e':
		case 'E':
			level = 4;
			break;
		case 'f':
		case 'F':
			level = 5;
			break;
			default:
				throw new RuntimeException("Unknown log level type: \""+levelStr+"\"");
		}
		return true;
	}
  
}

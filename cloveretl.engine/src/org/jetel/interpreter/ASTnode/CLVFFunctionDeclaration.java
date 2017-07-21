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


import org.jetel.interpreter.ExpParser;
import org.jetel.interpreter.TransformLangParserVisitor;

public class CLVFFunctionDeclaration extends SimpleNode {
    
    public String name;  
    public String[] varNames;
    public int[] varTypes;
    public int numParams;
  
  private static final int MAX_FUNCTION_PARAMETERS = 32; // maximum number of supported parameters
    
  public CLVFFunctionDeclaration(int id) {
    super(id);
    varNames=new String[MAX_FUNCTION_PARAMETERS];
    numParams=0;
  }

  public CLVFFunctionDeclaration(ExpParser p, int id) {
    super(p, id);
    varNames=new String[MAX_FUNCTION_PARAMETERS];
    numParams=0;
  }


  /** Accept the visitor. **/
  public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  public void setName(String name){
      this.name=name;
  }
  
  public void addVarName(int order,String _name){
      varNames[order]=_name;
      numParams++;
  }
  
  public void addVarType(int order,int type) {
      varTypes[order]=type;
  }
  
  public String toString(){
      int count=0;
      StringBuilder buffer=new StringBuilder(super.toString()).append(": ");
      while (varNames[count]!=null){
          buffer.append(varNames[count++]);
          buffer.append(",");
      }
      return buffer.toString();
  }
  
}

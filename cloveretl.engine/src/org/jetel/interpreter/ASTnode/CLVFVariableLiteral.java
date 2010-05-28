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
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangParserConstants;
import org.jetel.interpreter.TransformLangParserVisitor;
	

public class CLVFVariableLiteral extends SimpleNode implements TransformLangParserConstants{
    
    public int varSlot;
    public String varName;
    public boolean localVar;
    public int varType;
    public int arrayIndex=-1;
    public String mapKey;
    public boolean indexSet=false;
    public boolean scalarContext=false;
    public String fieldID;
    
  public CLVFVariableLiteral(int id) {
    super(id);
  }

  public CLVFVariableLiteral(ExpParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  public void setVarSlot(int slot){
      this.varSlot=slot;
  }
  
  public void setVarName(String name){
      this.varName=name;
  }
  
  public void setLocalVariale(boolean isLocal){
      this.localVar=isLocal;
  }
  
  public void setVarType(int type) {
      this.varType=type;
  }
  
  public String toString(){
      return super.toString()+" name \""+varName+"\" type "+tokenImage[varType]+" slot "+varSlot+" local "+localVar;
  }

  
/**
 * @param arrayIndex the arrayIndex to set
 * @since 21.3.2007
 */
public void setArrayIndex(String sIndex) throws ParseException {
    try{
        this.arrayIndex=Integer.parseInt(sIndex);
    }catch(NumberFormatException ex){
        throw new ParseException("Error when parsing \"arrayIndex\" parameter value \""+sIndex+"\"");
    }
}

/**
 * @param mapKey the mapKey to set
 * @since 21.3.2007
 */
public void setMapKey(String mapKey) {
    this.mapKey = mapKey;
}

public void setFieldID(String id){
	this.fieldID=id;
}

}

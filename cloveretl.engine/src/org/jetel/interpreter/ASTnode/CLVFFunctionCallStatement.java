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
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.extensions.TLContext;
import org.jetel.interpreter.extensions.TLFunctionPrototype;

 

public class CLVFFunctionCallStatement extends SimpleNode {
  
    public  String name;
    public CLVFFunctionDeclaration callNode;
    public TLFunctionPrototype externalFunction;
    public TLContext context;
    public TLValue[] externalFunctionParams;
    
  public CLVFFunctionCallStatement(int id) {
    super(id);
  }

  public CLVFFunctionCallStatement(ExpParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  @Override
public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  public void setName(String name){
      this.name=name;
  }
  
  public void setCallNode(CLVFFunctionDeclaration callNode){
      this.callNode=callNode;
  }
  
  public void setExternalFunction(TLFunctionPrototype function) {
      this.externalFunction=function;
  }
  
  @Override public void init() {
	  	super.init();
          if (externalFunction!=null) {
              this.context=externalFunction.createContext();
              this.externalFunctionParams=new TLValue[jjtGetNumChildren()];
              //may not work for some: this.externalFunctionParams=new TLValue[requiredNumParams()];
          }
  }
  
  @Override public String toString() {
      return super.toString()+" name \""+name+"\" min #params: "+requiredNumParams();
  }
  
  public final int requiredNumParams() {
      if (externalFunction!=null)
          return externalFunction.getParameterTypes().length;
      else if (callNode!=null)
          return callNode.numParams;
      else
          return 0;
  }
  
  public final boolean validateParams() {
      if (externalFunction!=null) {
          int numParams=jjtGetNumChildren();
          int maxParams=externalFunction.getMaxParams();
          int minParams=externalFunction.getMinParams();
//		TODO test lengts
//          int definedParams=externalFunction.getParameterTypes().length;
          
          return (numParams>=minParams && numParams<=maxParams);
      }
      return jjtGetNumChildren()==callNode.numParams;
  }
}

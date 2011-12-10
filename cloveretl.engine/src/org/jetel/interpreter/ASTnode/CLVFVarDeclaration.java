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
import org.jetel.interpreter.Token;
import org.jetel.interpreter.TransformLangParserConstants;
import org.jetel.interpreter.TransformLangParserVisitor;
import org.jetel.metadata.DataRecordMetadata;

public class CLVFVarDeclaration extends SimpleNode implements TransformLangParserConstants{
 
    public int type;
    public int varSlot; 
    public String name;
    public boolean localVar;
    public boolean hasInitValue;
    public int length;
	public int precision;
	public String metadataId;
	public int recordNo=-1;
    
    public CLVFVarDeclaration(int id) {
    super(id);
  }

  public CLVFVarDeclaration(ExpParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  @Override
public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  public void setType(int type){
      this.type=type;
  }
  
  public int getType() {
      return type;
  }
  
  public void setVarSlot(int slot){
      this.varSlot=slot;
  }
  
  public void setName(String name){
      this.name=name;
  }
  
  public void setLocalVariale(boolean isLocal){
      this.localVar=isLocal;
  }
  
  
  @Override
public String toString(){
      return super.toString()+" name \""+name+"\" type "+tokenImage[type];
  }
    
  public void hasInitValue(boolean init){
	  this.hasInitValue=init;
  }
  
  public void setLength(String lengthString) throws ParseException{
      try{
          this.length=Integer.parseInt(lengthString);
      }catch(NumberFormatException ex){
          throw new ParseException("Error when parsing \"length\" parameter value \""+lengthString+"\"");
      }
  }
  
  public void setPrecision(String precisionString) throws ParseException{
      try{
          this.precision=Integer.parseInt(precisionString);
      }catch(NumberFormatException ex){
          throw new ParseException("Error when parsing \"precision\" parameter value \""+precisionString+"\"");
      }
  }
	
  public void setMetadataId(String id){
	  this.metadataId=id;
  }
  
  public void setRecordNum(Token t,String fRecNum) throws ParseException { 
 	 recordNo=Integer.parseInt(fRecNum.substring(1));
      DataRecordMetadata record=parser.getInRecordMeta(recordNo);
      if (record==null){
          throw new ParseException(t,"Input record number ["+recordNo+"] does not exist"); 
      }
  }
  
  public void setRecordName(Token t,String recName) throws ParseException { 
 	 recordNo=parser.getInRecordNum(recName.substring(1));
      if (recordNo<0){
          throw new ParseException(t,"Input record name ["+recName+"] does not exist"); 
      }
  }
    
}

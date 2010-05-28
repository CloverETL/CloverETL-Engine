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
import org.jetel.component.CustomizedRecordTransform;
import org.jetel.interpreter.ExpParser;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangParserVisitor;
import org.jetel.interpreter.data.TLValue;
import org.jetel.metadata.DataRecordMetadata;

public class CLVFWildCardMapping extends SimpleNode {
 
  public TLValue nodeVal;
  public CustomizedRecordTransform custTrans;
  public boolean initialized=false;
  
  public CLVFWildCardMapping(int id) {
    super(id);
  }

  public CLVFWildCardMapping(ExpParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
	  return visitor.visit(this, data);
  }
  
  public void setRule(String outRec, String inRec,CustomizedRecordTransform custTrans) throws ParseException {
	  String outRecId=outRec.substring(1, outRec.indexOf('.'));
	  String inRecId=inRec.substring(1, inRec.indexOf('.'));
	  // validate OUTrecord first
	  if (Character.isDigit(outRecId.charAt(0))){
		  int recordNo=Integer.parseInt(outRecId);
	       DataRecordMetadata record=parser.getOutRecordMeta(recordNo);
	       if (record==null){
	           throw new ParseException("Unknown output record ["+outRec+"]"); 
	       }
	  }else{
		  DataRecordMetadata record;
		  try{
	     	  int recordNo=parser.getOutRecordNum(outRecId);
	     	  record=parser.getOutRecordMeta(recordNo);
	       }catch(Exception ex){
	           throw new ParseException("Error accessing record \""+outRecId+"\" "+ex.getMessage());
	       }
	       if (record==null){
	           throw new ParseException("Unknown output record \""+outRecId+"\""); 
	       }
		  
	  }
	  // IN record
	  if (Character.isDigit(inRecId.charAt(0))){
		  int recordNo=Integer.parseInt(inRecId);
	       DataRecordMetadata record=parser.getInRecordMeta(recordNo);
	       if (record==null){
	           throw new ParseException("Unknown input record ["+inRec+"]"); 
	       }
	  }else{
		  DataRecordMetadata record;
		  try{
	     	  int recordNo=parser.getInRecordNum(inRecId);
	     	  record=parser.getInRecordMeta(recordNo);
	       }catch(Exception ex){
	           throw new ParseException("Error accessing record \""+inRecId+"\" "+ex.getMessage());
	       }
	       if (record==null){
	           throw new ParseException("Unknown in record \""+inRecId+"\""); 
	       }
	  }
	  StringBuffer outPattern=new StringBuffer().append("${").append(outRec.substring(1)).append("}");
	  StringBuffer inPattern=new StringBuffer().append("${").append(inRec.substring(1)).append("}");
	  custTrans.addFieldToFieldRule(outPattern.toString(), inPattern.toString());
	  this.custTrans=custTrans;
  }
  
  
}

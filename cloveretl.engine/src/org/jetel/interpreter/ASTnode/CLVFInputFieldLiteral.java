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
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.interpreter.Token;
import org.jetel.interpreter.ExpParser;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.TransformLangParserVisitor;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

public class CLVFInputFieldLiteral extends SimpleNode {
	
	public DataField field;
    public int recordNo=-1;
    public int fieldNo=-1;
    public String fieldName;
    public boolean indexSet=false;
	
	public CLVFInputFieldLiteral(int id) {
		super(id);
	}
	
	public CLVFInputFieldLiteral(ExpParser p, int id) {
		super(p, id);
	}
	
	/** Accept the visitor. **/
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}
	
	 @Override public String toString() {
	      return super.toString()+" record# \""+recordNo+"\" field# "+fieldNo+" field name: "+fieldName;
	  }
	
	/**
     * Get field of input record (1st record)
     * 
	 * @param fName
	 * @throws ParseException
	 */
	public void setFieldName(Token t,String fName) throws ParseException{
		// get rid of leading '$' character (the 1st character)
		fieldName=fName;
        recordNo=0;
        DataRecordMetadata record= parser.getInRecordMeta();
        if (record==null){
            throw new ParseException(t,"Unknown data field ["+fName+"]");
        }
		fieldNo=record.getFieldPosition(fName.substring(1));
		if (fieldNo==-1){
			throw new ParseException(t,"Unknown input field ["+fName+"]");
		}
	}
     public void setRecordFieldName(Token t,String fRecName) throws ParseException{
            // get rid of leading '$' character (the 1st character)
            String recFieldName[]=fRecName.substring(1).split("\\.");
            fieldName=recFieldName[1];
            recordNo=parser.getInRecordNum(recFieldName[0]);
            DataRecordMetadata record=parser.getInRecordMeta(recordNo);
            if (record==null){
                throw new ParseException(t,"Unknown input field ["+fRecName+"]"); 
            }
            fieldNo=record.getFieldPosition(recFieldName[1]);
            if (fieldNo==-1){
                throw new ParseException(t,"Unknown input field ["+fRecName+"]");
            }
        }
     
     public void setRecordNameFieldNum(Token t,String fRecName) throws ParseException{
         // get rid of leading '$' character (the 1st character)
         String recFieldName[]=fRecName.substring(1).split("\\.");
         fieldName=recFieldName[1];
         recordNo=parser.getInRecordNum(recFieldName[0]);
         DataRecordMetadata record=parser.getInRecordMeta(recordNo);
         if (record==null){
             throw new ParseException(t,"Unknown input field ["+fRecName+"]"); 
         }
         try {
        	 fieldNo=Integer.parseInt(recFieldName[1]);
         } catch(NumberFormatException ex){
        	 throw new ParseException(t,"Unknown input field ["+fRecName+"]");
         }
    	 DataFieldMetadata field = record.getField(fieldNo);
    	 if (field==null)
    		 throw new ParseException(t,"Unknown input field ["+fRecName+"]");
     }
     
     public void setRecordNumFieldName(Token t,String fRecName) throws ParseException{
         // get rid of leading '$' character (the 1st character)
         String recFieldName[]=fRecName.substring(1).split("\\.");
         recordNo=Integer.parseInt(recFieldName[0]);
         DataRecordMetadata record=parser.getInRecordMeta(recordNo);
         if (record==null){
             throw new ParseException(t,"Unknown input field ["+fRecName+"]"); 
         }
         fieldNo=record.getFieldPosition(recFieldName[1]);
         if (fieldNo==-1){
             throw new ParseException(t,"Unknown input field ["+fRecName+"]");
         }
         fieldName=record.getField(fieldNo).getName();
     }   
     
     
     public void setRecordNumFieldNum(Token t,String fRecFieldNum) throws ParseException { 
    	 String items[]=fRecFieldNum.substring(1).split("\\.");
    	 recordNo=Integer.parseInt(items[0]);
         DataRecordMetadata record=parser.getInRecordMeta(recordNo);
         if (record==null){
             throw new ParseException(t,"Unknown record number ["+fRecFieldNum+"]"); 
         }
         try{
        	 fieldNo=Integer.parseInt(items[1]);
        	 DataFieldMetadata field = record.getField(fieldNo);
        	 if (field==null)
        		 throw new ParseException(t,"Unknown input field ["+fRecFieldNum+"]");
        	 fieldName=field.getName();
         }catch(Throwable ex){
             throw new ParseException(t,"Unknown input field ["+fRecFieldNum+"]");
         }
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
             throw new ParseException(t,"Input record name ["+recName.substring(1)+"] does not exist"); 
         }
     }
     
     
     public void bindToField(DataRecord[] records){
         try{
             field=records[recordNo].getField(fieldNo);
         }catch(NullPointerException ex){
             throw new TransformLangExecutorRuntimeException("can't determine "+fieldName);
         }
     }
	
}

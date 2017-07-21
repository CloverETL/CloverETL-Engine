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

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.interpreter.Stack;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.TransformLangParserVisitor;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

public class CLVFLookupNode extends SimpleNode {
    
    public static final int OP_GET=0;
    public static final int OP_NEXT=1;
    public static final int OP_NUM_FOUND=2;
    public static final int OP_INIT=3;
    public static final int OP_FREE=4;
    
    public String lookupName;
    public String fieldName;
    public int opType=0; //default is get
    public  LookupTable lookupTable;
    public  Lookup lookup;
    private DataRecord lookupRecord;
    public int fieldNum=-1; //lazy initialization
    
  public CLVFLookupNode(int id) {
    super(id);
  }

  public CLVFLookupNode(TransformLangParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  @Override
public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }
  
  public void setLookupName(String name){
      this.lookupName=name;
  }
  
  public void setLookupField(String name){
      this.fieldName=name;
      
  }
  
  public void setOperation(int oper){
      this.opType=oper;
  }
  
	  /**
	   * Creates lookup based on requested keys - corresponds to {@link LookupTable#createLookup(RecordKey)} 
	   * 
	 * @param keys keys for lookup
	 * @throws ComponentNotReadyException 
	 */
	public void createLookup(Stack stack) throws ComponentNotReadyException{
	  //create lookup key and lookup record based on requested keys: record contains only key fields
	  DataRecordMetadata lookupMetadata = new DataRecordMetadata("_metadata_ctl_" + lookupTable.getName());
	  final int numKeys=jjtGetNumChildren();
	  int[] keyFields = new int[numKeys];
	  for (int i = numKeys-1; i >=0; i--) {
		lookupMetadata.addField(new DataFieldMetadata("_field" + i, TLValueType.convertType(stack.get(i).getType()), null));
		keyFields[i] = i;
	  }
	  RecordKey lookupKey = new RecordKey(keyFields, lookupMetadata);
	  lookupRecord = new DataRecord(lookupMetadata);
	  lookupRecord.init();
	  lookup = lookupTable.createLookup(lookupKey, lookupRecord);
  }
	
  /**
	 * For new lookup node, which is to work on existing lookup, creates lookup record, based on lookup key
	 * 
	 */
	private void prepareLookupRecord() {
		lookupRecord = new DataRecord(lookup.getKey().generateKeyRecordMetadata());
		lookupRecord.init();
	}
  
	  /**
	   *Performs lookup in lookup table - corresponds to {@link Lookup#seek(DataRecord)}
	   * 
	 * @param keys keys for lookup
	 */
	public void seek(Stack stack){
		
		if (lookupRecord == null) {
			  prepareLookupRecord();
		  }
		  for (int i = jjtGetNumChildren()-1; i>=0; i--) {
			    stack.pop().copyToDataField(lookupRecord.getField(i));
			}
		  lookup.seek(lookupRecord);
	  }
}

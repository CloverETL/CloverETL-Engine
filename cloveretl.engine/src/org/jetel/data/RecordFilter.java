/*
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.data;

import java.util.*;
import java.io.*;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.database.SQLUtil;
import java.util.*;
import java.lang.reflect.*;
import java.util.Hashtable;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public class RecordFilter{
	private DataRecordMetadata metadata;
	private Vector filterSpecs;


	public RecordFilter(String filterExpression)
	{
		filterSpecs = new Vector();
		metadata = metadata;
		String [] filterParts=filterExpression.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		String filterPart;
		String filterField;
		String filterValue;
		String comparisonOperator = new String("");
		for (int i=0; i<Array.getLength(filterParts); i++)
		{
			filterPart = filterParts[i];

            //Get comparison operator
			if (filterPart.indexOf("=")!=-1){
				comparisonOperator=new String("=");
			}
			else if (filterPart.indexOf("~")!=-1){
				comparisonOperator=new String("~");
			}
			else if (filterPart.indexOf(">")!=-1){
				comparisonOperator=new String(">");
			}
			else if (filterPart.indexOf("<")!=-1){
				comparisonOperator=new String("<");
			}
				
				
			
			filterField = filterPart.substring(0, filterPart.indexOf(comparisonOperator));
			filterValue = filterPart.substring(filterPart.indexOf(comparisonOperator)+1,filterPart.length());
			Hashtable filterSpec = new Hashtable();
			filterSpec.put("field",filterField);
			filterSpec.put("value",filterValue);
			filterSpec.put("comparison",comparisonOperator);
			filterSpecs.add(filterSpec);
		}
		


	}
	public Vector getFilterSpecs()
	{
		return filterSpecs;
	}

	public boolean accepts(DataRecord record)
	{
		Enumeration specsEnumeration = filterSpecs.elements();
		//iterate through each filter spec. Once a spec is verified, the record is accepted (it's an OR combination of all specs)
		while (specsEnumeration.hasMoreElements())
		{
			Hashtable filterSpec = (Hashtable) specsEnumeration.nextElement();
		    String fieldType = SQLUtil.jetelType2Str(record.getField((String) filterSpec.get("field")).getType());
			//string comparison
			if (fieldType.compareTo("string")==0)
			{
				StringBuffer fieldValue =   (StringBuffer)  record.getField((String) filterSpec.get("field")).getValue();

				//logically, if we filter on a node, it's because it has a non-null value
				if (fieldValue==null){
					return false;
				}
				
				String comparison = (String) filterSpec.get("comparison");
				String refValue = (String) filterSpec.get("value");
				//equality
				if ( comparison.compareTo("=")==0)
				{
					if (fieldValue.toString().compareTo(refValue)==0)
					{
						return true;
					}
				}
				//regexp
				else if ( comparison.compareTo("~")==0)
					if (Pattern.matches(refValue, fieldValue ))
					{
						return true;
					}
				//greater than
				else if ( comparison.compareTo(">")==0)
					if (fieldValue.toString().compareTo(refValue)>0)
					{
						return true;
					}
				//lower than
				else if ( comparison.compareTo("<")==0)
					if (fieldValue.toString().compareTo(refValue)<0)
					{
						return true;
					}
			}
			//integer fields
			else if (fieldType.compareTo("integer")==0)
			{
				//getting filter values
				Integer fieldValue = (Integer) record.getField((String) filterSpec.get("field")).getValue();

				//logically, if we filter on a node, it's because it has a non-null value
				if (fieldValue==null){
					return false;
				}
				
				String comparison = (String) filterSpec.get("comparison");
				Integer refValue = new Integer(Integer.parseInt((String) filterSpec.get("value")));

				//check equality
				if ( comparison.compareTo("=")==0)
				{
					if (fieldValue.equals(refValue))
					{
						return true;
					}
				}
				//check lower than
				else if ( comparison.compareTo("<")==0)
				{
					if (fieldValue.compareTo(refValue)<0)
					{
						return true;
					}
				}
				//check higher than
				else if ( comparison.compareTo(">")==0)
				{
					if (fieldValue.compareTo(refValue)>0)
					{
						return true;
					}
				}
			}
			//date fields 
			else if (fieldType.compareTo("date")==0)
			{
				try{
					
					//getting filter value
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					Date fieldValue = (Date) record.getField((String) filterSpec.get("field")).getValue();

					//logically, if we filter on a node, it's because it has a non-null value
					if (fieldValue==null){
						return false;
					}
				
					String comparison = (String) filterSpec.get("comparison");
					Date refValue = df.parse((String) filterSpec.get("value"));

					//check equality
					if ( comparison.compareTo("=")==0)
					{
						if (fieldValue.equals(refValue))
						{
							return true;
						}
					}
					//check lower than
					else if ( comparison.compareTo("<")==0)
					{
						if (fieldValue.compareTo(refValue)<0)
						{
							return true;
						}
					}
					//check higher than
					else if ( comparison.compareTo(">")==0)
					{
						if (fieldValue.compareTo(refValue)>0)
						{
							return true;
						}
				}

				}
				catch (Exception e){
				    e.printStackTrace();
				}

			}

  	}
	
		return false;

	}

}

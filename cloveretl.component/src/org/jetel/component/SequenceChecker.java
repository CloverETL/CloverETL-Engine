/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
package org.jetel.component;

import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Map;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordComparator;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.MiscUtils;
import org.w3c.dom.Element;
/**
 *  <h3>Sequence Checker Component</h3>
 *
 * <!-- Checks the incoming records based on specified key whather all records are in properly order-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Sort</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Checks the incoming records based on specified key whather all records are in properly order.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  The sort order is either Ascending (default) or Descending.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>Zero or more connected output ports.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"SEQUENCE_CHECKER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>sortKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>sortOrder</b><br><i>optional</i></td><td>one of "Ascending|Descending" {the fist letter is sufficient, if not defined, then Ascending}</td></tr>
 *  <tr><td><b>uniqueKeys</b><br><i>optional</i></td><td>true/false permit duplicate keys</td></tr>
 *  <tr><td><b>useI18N</b><br><i>optional</i></td><td>true/false perform sorting according to national rules - e.g. Czech or German handling of characters like "i","í". Default
 *  is false.<br>Use it only if you are sorting data according to key which can contain accented characters or
 *  you want sorter to follow certain locale specific rules.</td></tr>
 *  <tr><td><b>locale</b><br><i>optional</i></td><td>locale to be used when sorting using I18N rules. If not specified, then system
 *  default is used.<br><i>Example: "fr"</i></td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="SEQUENCE_CHECKER" type="SEQUENCE_CHECKER" sortKey="Name" sortOrder="Descending" /&gt;</pre>
 *  
 *  <pre>&lt;Node id="SEQUENCE_CHECKER" type="SEQUENCE_CHECKER" sortKey="Name" sortOrder="Descending" uniqueKeys="true" useI18N="true" locale="EN"/&gt;</pre>
 *
 * @author      Jan Ausperger
 * @since       January 5, 2007
 * @revision    $Revision:  $
 */
public class SequenceChecker extends Node {

	private static final String XML_SORTORDER_ATTRIBUTE = "sortOrder";
	private static final String XML_SORTKEY_ATTRIBUTE = "sortKey";
	//private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
	private static final String XML_UNIQUE_ATTRIBUTE = "uniqueKeys";
    private static final String XML_USE_I18N_ATTRIBUTE = "useI18N";
    private static final String XML_LOCALE_ATTRIBUTE = "locale";
    
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "SEQUENCE_CHECKER";

	private final static int READ_FROM_PORT = 0;

	private String keyFieldNames[];
	private boolean sortOrderAscending;
	private RecordComparator recordComparator;
	//private boolean equalNULLs = true;
	private boolean uniqueKeys = false;
    private String localeStr = null;
    private boolean useI18N;

	private final static boolean DEFAULT_ASCENDING_SORT_ORDER = true; 

	/**
	 *Constructor for the SequenceChecker object
	 *
	 * @param  id               Description of the Parameter
	 * @param  keyFieldNames    Description of the Parameter
	 * @param  sortOrder        Description of the Parameter
	 */
	public SequenceChecker(String id, String[] keyFieldNames, boolean sortOrder) {
		super(id);
		this.sortOrderAscending = sortOrder;
		this.keyFieldNames = keyFieldNames;
	}

	/**
	 *Constructor for the SequenceChecker object
	 *
	 * @param  id               Description of the Parameter
	 * @param  keyFieldNames    Description of the Parameter
	 */
	public SequenceChecker(String id, String[] keyFieldNames) {
		this(id,keyFieldNames,DEFAULT_ASCENDING_SORT_ORDER);
	}

	@Override
	public Result execute() throws Exception {
		int current = 1;
		int previous = 0;
		int compareResult;
		boolean isFirst = true; // special treatment for 1st record
		InputPort inPort = getInputPort(READ_FROM_PORT);
		boolean isOutPort = !getOutPorts().isEmpty();
		DataRecord[] records = {new DataRecord(inPort.getMetadata()), new DataRecord(inPort.getMetadata())};
		records[0].init();
		records[1].init();
		
		while ((records[current] = inPort.readRecord(records[current])) != null && runIt) {
			if (isFirst) {
				isFirst = false;
			} else {
				compareResult = recordComparator.compare(records[current], records[previous]);

				if (compareResult == 0) {
					if (uniqueKeys) 
						return Result.ERROR;
				} else if (compareResult > 0) {
					if (!sortOrderAscending) 
						return Result.ERROR;
				} else if (sortOrderAscending) {
					return Result.ERROR;
				}
			}
			
			if (isOutPort) writeRecordBroadcast(records[current]);

			// swap indexes
			current = current ^ 1;
			previous = previous ^ 1;
		}
		if (isOutPort) broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void free() {
		super.free();
	}
	/**
	 *  Sets the keyOrderAscending attribute of the SequenceChecker object
	 *
	 * @param  ascending  The new sortOrderAscending value
	 */
	public void setSortOrderAscending(boolean ascending) {
		sortOrderAscending = ascending;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  January 5, 2007
	 */
	public void init() throws ComponentNotReadyException {
		super.init();
		int keyFields[];
        Integer position;
        keyFields = new int[keyFieldNames.length];
        Map fields = getInputPort(READ_FROM_PORT).getMetadata().getFieldNames();

        for (int i = 0; i < keyFieldNames.length; i++) {
            if ((position = (Integer) fields.get(keyFieldNames[i])) != null) {
                keyFields[i] = position.intValue();
            } else {
                throw new RuntimeException(
                        "Field name specified as a key doesn't exist: "
                                + keyFieldNames[i]);
            }
        }
	    
		recordComparator = new RecordComparator(keyFields);
		//recordComparator.setEqualNULLs(equalNULLs);
		
        if (useI18N){
            recordComparator=new RecordComparator(keyFields,(RuleBasedCollator)Collator.getInstance(MiscUtils.createLocale(localeStr)));
        }else{
        	recordComparator=new RecordComparator(keyFields);
        }
	}

	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     January 5, 2007
	 */
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (keyFieldNames != null) {
			StringBuffer buf = new StringBuffer(keyFieldNames[0]);
			for (int i=1; i< keyFieldNames.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + keyFieldNames[i]); 
			}
			xmlElement.setAttribute(XML_SORTKEY_ATTRIBUTE,buf.toString());
		}
		if (sortOrderAscending == false) {
			xmlElement.setAttribute(XML_SORTORDER_ATTRIBUTE, "Descending");
		}
        
        if (useI18N){
            xmlElement.setAttribute(XML_USE_I18N_ATTRIBUTE, String.valueOf(useI18N));
        }
        
        if (localeStr!=null){
            xmlElement.setAttribute(XML_LOCALE_ATTRIBUTE, localeStr);
        }

        // equal NULL attribute
		//xmlElement.setAttribute(XML_EQUAL_NULL_ATTRIBUTE, String.valueOf(equalNULLs));
		
		// unique keys attribute
		xmlElement.setAttribute(XML_UNIQUE_ATTRIBUTE, String.valueOf(uniqueKeys));

	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @since           January 5, 2007
	 */
	   public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		SequenceChecker checker;
		try {
			checker = new SequenceChecker(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_SORTKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			if (xattribs.exists(XML_SORTORDER_ATTRIBUTE)) {
				checker.setSortOrderAscending(xattribs.getString(XML_SORTORDER_ATTRIBUTE).matches("^[Aa].*"));
			}
			/*if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
				checker.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
			}*/
			if (xattribs.exists(XML_UNIQUE_ATTRIBUTE)){
				checker.setUnique(xattribs.getBoolean(XML_UNIQUE_ATTRIBUTE));
			}
            if (xattribs.exists(XML_USE_I18N_ATTRIBUTE)){
            	checker.setUseI18N(xattribs.getBoolean(XML_USE_I18N_ATTRIBUTE));
            }
            if (xattribs.exists(XML_LOCALE_ATTRIBUTE)){
            	checker.setLocaleStr(xattribs.getString(XML_LOCALE_ATTRIBUTE));
            }
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
		return checker;
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
        @Override
        public ConfigurationStatus checkConfig(ConfigurationStatus status) {
    		super.checkConfig(status);
   		 
    		checkInputPorts(status, 1, 1);
            checkOutputPorts(status, 0, Integer.MAX_VALUE);

            try {
                init();
                free();
            } catch (ComponentNotReadyException e) {
                ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
                if(!StringUtils.isEmpty(e.getAttributeName())) {
                    problem.setAttributeName(e.getAttributeName());
                }
                status.add(problem);
            }
            
            return status;
       }
	
	public String getType(){
		return COMPONENT_TYPE;
	}

	/*public void setEqualNULLs(boolean equal){
	    this.equalNULLs=equal;
	}*/

	public void setUnique(boolean unique){
	    this.uniqueKeys=unique;
	}
	
    /**
     * @return the localeStr
     */
    public String getLocaleStr() {
        return localeStr;
    }


    /**
     * @param localeStr the localeStr to set
     */
    public void setLocaleStr(String localeStr) {
        this.localeStr = localeStr;
    }


    /**
     * @return the useI18N
     */
    public boolean isUseI18N() {
        return useI18N;
    }


    /**
     * @param useI18N the useI18N to set
     */
    public void setUseI18N(boolean useI18N) {
        this.useI18N = useI18N;
    }
	
}


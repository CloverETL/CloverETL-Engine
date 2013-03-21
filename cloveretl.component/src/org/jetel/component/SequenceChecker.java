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
package org.jetel.component;

import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.RecordComparator;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.CopyComponentTokenTracker;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MiscUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
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
 *  <tr><td><b>useI18N</b><br><i>optional</i></td><td>true/false perform sorting according to national rules - e.g. Czech or German handling of characters like "i","??". Default
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

    static Log logger = LogFactory.getLog(SequenceChecker.class);

	private static final String XML_SORTORDER_ATTRIBUTE = "sortOrder";
	private static final String XML_SORTKEY_ATTRIBUTE = "sortKey";
	private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
	private static final String XML_UNIQUE_ATTRIBUTE = "uniqueKeys";
    private static final String XML_USE_I18N_ATTRIBUTE = "useI18N";
    private static final String XML_LOCALE_ATTRIBUTE = "locale";
    
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "SEQUENCE_CHECKER";

	private final static int READ_FROM_PORT = 0;

	private String[] keyFieldNames;
	private boolean[] sortOrderings;
	private RecordComparator recordComparator;
	private boolean equalNULL = true;
	private boolean uniqueKeys = false;
	
	@Deprecated
    private String localeStr = null;
	@Deprecated
    private boolean useI18N;

	private final static boolean DEFAULT_ASCENDING_SORT_ORDER = true; 

	/**
	 *Constructor for the SequenceChecker object
	 *
	 * @param  id               Description of the Parameter
	 * @param  keyFieldNames    Description of the Parameter
	 * @param  oldAscendingOrder        Description of the Parameter
	 */
	public SequenceChecker(String id, String[] keyFieldNames, boolean oldAscendingOrder) {
		super(id);
        this.sortOrderings = new boolean[keyFieldNames.length];
        Arrays.fill(sortOrderings, oldAscendingOrder);
		
        Pattern pat = Pattern.compile("^(.*)\\((.*)\\)$");
        
        for (int i = 0; i < keyFieldNames.length; i++) {
        	Matcher matcher = pat.matcher(keyFieldNames[i]);
        	if (matcher.find()) {
	        	String keyPart = keyFieldNames[i].substring(matcher.start(1), matcher.end(1));
	        	if (matcher.groupCount() > 1) {
	        		sortOrderings[i] = (keyFieldNames[i].substring(matcher.start(2), matcher.end(2))).matches("^[Aa].*");	        		
	        	}
	        	keyFieldNames[i] = keyPart;
        	}
        }
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
		DataRecord[] records = {DataRecordFactory.newRecord(inPort.getMetadata()), DataRecordFactory.newRecord(inPort.getMetadata())};
		records[0].init();
		records[1].init();
		boolean error = false; 
		int row = 1;
		
		while ((records[current] = inPort.readRecord(records[current])) != null && runIt) {
			if (isFirst) {
				isFirst = false;
			} else {
				compareResult = recordComparator.compare(records[previous], records[current]);

				if (compareResult == 0) {
					if (uniqueKeys) {
						error = true;
						break;
					}
				} else if (compareResult > 0) {
					error = true;
					break;
				}
			}
			
			if (isOutPort) writeRecordBroadcast(records[current]);

			// swap indexes
			current = current ^ 1;
			previous = previous ^ 1;
			
			row++;
		}
		if (isOutPort) broadcastEOF();
		
		Result result = null;
		if (error) {
			throw new JetelException("The sequence checker fails at row '" + row + "'.");
		} else {
			result = Result.FINISHED_OK;
		}
        return runIt ? result : Result.ABORTED;
	}

	@Override
	public void free() {
        if(!isInitialized()) return;
		super.free();
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of the Exception
	 * @since                                  January 5, 2007
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		int keyFields[];
        Integer position;
        keyFields = new int[keyFieldNames.length];
        DataRecordMetadata metadata = getInputPort(READ_FROM_PORT).getMetadata();
        Map fields = metadata.getFieldNamesMap();

        for (int i = 0; i < keyFieldNames.length; i++) {
            if ((position = (Integer) fields.get(keyFieldNames[i])) != null) {
                keyFields[i] = position.intValue();
            } else {
                throw new RuntimeException(
                        "Field name specified as a key doesn't exist: "
                                + keyFieldNames[i]);
            }
        }
	    
        if (useI18N){
            recordComparator=new RecordComparator(keyFields,(RuleBasedCollator)Collator.getInstance(MiscUtils.createLocale(localeStr)));
        }else{
        	recordComparator=new RecordComparator(keyFields);
        }
        recordComparator.setSortOrderings(sortOrderings);
		recordComparator.setEqualNULLs(equalNULL);
        recordComparator.updateCollators(metadata);
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Returned Value
	 * @since     January 5, 2007
	 */
	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (keyFieldNames != null) {
			StringBuffer buf = new StringBuffer(keyFieldNames[0]);
			for (int i=1; i< keyFieldNames.length; i++) {
				buf.append(Defaults.Component.KEY_FIELDS_DELIMITER + keyFieldNames[i]).
				append("(").append(sortOrderings[i]).append(")"); 
			}
			xmlElement.setAttribute(XML_SORTKEY_ATTRIBUTE,buf.toString());
		}
        
        if (useI18N){
            xmlElement.setAttribute(XML_USE_I18N_ATTRIBUTE, String.valueOf(useI18N));
        }
        
        if (localeStr!=null){
            xmlElement.setAttribute(XML_LOCALE_ATTRIBUTE, localeStr);
        }

        // equal NULL attribute
		xmlElement.setAttribute(XML_EQUAL_NULL_ATTRIBUTE, String.valueOf(equalNULL));
		
		// unique keys attribute
		xmlElement.setAttribute(XML_UNIQUE_ATTRIBUTE, String.valueOf(uniqueKeys));

	}


	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since           January 5, 2007
	 */
	   public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		SequenceChecker checker;
		checker = new SequenceChecker(xattribs.getString(XML_ID_ATTRIBUTE),
				xattribs.getString(XML_SORTKEY_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX),
				xattribs.getString(XML_SORTORDER_ATTRIBUTE, "A").matches("^[Aa].*"));
		if (xattribs.exists(XML_UNIQUE_ATTRIBUTE)){
			checker.setUnique(xattribs.getBoolean(XML_UNIQUE_ATTRIBUTE));
		}
        if (xattribs.exists(XML_USE_I18N_ATTRIBUTE)){
        	checker.setUseI18N(xattribs.getBoolean(XML_USE_I18N_ATTRIBUTE));
        }
        if (xattribs.exists(XML_LOCALE_ATTRIBUTE)){
        	checker.setLocaleStr(xattribs.getString(XML_LOCALE_ATTRIBUTE));
        }
        if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
        	checker.setEqualNULL(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
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
   		 
    		if(!checkInputPorts(status, 1, 1)
    				|| !checkOutputPorts(status, 0, Integer.MAX_VALUE)) {
    			return status;
    		}
    		
            checkMetadata(status, getInMetadata(), getOutMetadata());

            try {
                init();
            } catch (ComponentNotReadyException e) {
                ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
                if(!StringUtils.isEmpty(e.getAttributeName())) {
                    problem.setAttributeName(e.getAttributeName());
                }
                status.add(problem);
            } finally {
            	free();
            }
            
            return status;
       }
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}

	public void setEqualNULL(boolean equalNULL) {
	    this.equalNULL = equalNULL;
	}

	public void setUnique(boolean unique){
	    this.uniqueKeys=unique;
	}
	
    /**
     * @return the localeStr
     */
    @Deprecated
    public String getLocaleStr() {
        return localeStr;
    }


    /**
     * @param localeStr the localeStr to set
     */
    @Deprecated
    public void setLocaleStr(String localeStr) {
        this.localeStr = localeStr;
    }


    /**
     * @return the useI18N
     */
    @Deprecated
    public boolean isUseI18N() {
        return useI18N;
    }


    /**
     * @param useI18N the useI18N to set
     */
    @Deprecated
    public void setUseI18N(boolean useI18N) {
        this.useI18N = useI18N;
    }
	
    @Override
    protected ComponentTokenTracker createComponentTokenTracker() {
    	return new CopyComponentTokenTracker(this);
    }
    
}


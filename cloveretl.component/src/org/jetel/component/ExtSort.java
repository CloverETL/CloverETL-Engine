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

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.ExternalSortDataRecord;
import org.jetel.data.ISortDataRecord;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.CopyComponentTokenTracker;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;
/**
 *  <h3>Sort Component</h3>
 *
 * <!-- Sorts the incoming records based on specified key -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Sort</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Sorts the incoming records based on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  The sort order is either Ascending (default) or Descending.<br>
 * In case there is not enough room in internal sort buffer, it performs
 * external sorting - thus any number of internal records can be sorted.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one connected output port.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"EXT_SORT"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>sortKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>sortOrder</b><br><i>optional</i></td><td>one of "Ascending|Descending" {the fist letter is sufficient, if not defined, then Ascending}</td>
 *  <tr><td><b>numberOfTapes</b><br><i>optional</i></td><td>even number greater than 2 - denotes how many tapes (temporary files) will be used when external sorting data.
 *  <i>Default is 6 tapes.</i></td>
 *  <!--tr><td><b>sorterInitialCapacity</b><br><i>optional</i></td><td>the initial capacity of internal sorter used for in-memory sorting records. If the
 *   system has plenty of memory, specify high number here (5000 or more). If the system is short on memory, use low number (100).<br>
 *   The final capacity is based on following formula:<br><code>sorter_initial_capacity * (1 - grow_factor^max_num_collections)/(1 - grow_factor)</code><br>
 *   where:<br><code>grow_factor=1.6<br>max_num_collections=8<br>sorterInitialCapacity=2000<br></code><br>With the parameters above, the default total capacity roughly is <b>140000</b> records. The
 *   total capacity is approximately <code>69,91 * sorterInitialCapacity</code>.<br><br>
 *   Following tables shows Total Capacities of internal buffer for various Initial Capacity values:
 *   <table border="1">
 *   <tr><th>Initial Capacity</th><th>Total Capacity</th></tr>
 *    <tr><td>10</td><td>1000</td></tr>
 *    <tr><td>100</td><td>7000</td></tr>
 *    <tr><td>1000</td><td>70000</td></tr>
 *    <tr><td>2000</td><td>140000</td></tr>
 *    <tr><td>5000</td><td>350000</td></tr>
 *    <tr><td>10000</td><td>700000</td></tr>
 *    <tr><td>20000</td><td>1399000</td></tr>
 *    <tr><td>50000</td><td>3496000</td></tr>
 *    </table>
 *  </tr-->
 *  <tr><td><b>bufferCapacity</b><br><i>optional</i></td><td>What is the maximum number of records
 *  which are sorted in-memory. If number of records exceed this size, external sorting is performed.</td></tr>
 *  <tr><td><b>tmpDirs</b><br><i>optional</i></td><td>Semicolon (;) delimited list of directories which should be
 *  used for creating tape files - used when external sorting is performed. Default value is equal to Java's <code>java.io.tmpdir</code> system property.</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="SORT_CUSTOMER" type="EXT_SORT" sortKey="Name:Address" sortOrder="A"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 */
public class ExtSort extends Node {

	private static final String XML_NUMBEROFTAPES_ATTRIBUTE = "numberOfTapes";
	private static final String XML_SORTERINITIALCAPACITY_ATTRIBUTE = "sorterInitialCapacity";
	private static final String XML_SORTORDER_ATTRIBUTE = "sortOrder";
	private static final String XML_SORTKEY_ATTRIBUTE = "sortKey";
    private static final String XML_BUFFER_CAPACITY_ATTRIBUTE = "bufferCapacity";
     private static final String XML_LOCALE_ATTRIBUTE = "locale";
	private static final String XML_CASE_SENSITIVE_ATTRIBUTE = "caseSensitive";
    
    
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "EXT_SORT";

	private final static int READ_FROM_PORT = 0;

	private ISortDataRecord sorter;
//	private SortOrder sortOrderAscending;
	private boolean[] sortOrderings;
	private String[] sortKeysNames;    
	/*
	 * case sensitive sorting of string fields?
	 */
	boolean caseSensitive = true;

	
	private InputPort inPort;
	private DataRecord inRecord;

	private int internalBufferCapacity;
	private int numberOfTapes;
	private CloverBuffer recordBuffer;
	private String localeStr;

	private final static int DEFAULT_NUMBER_OF_TAPES = 6;
	private static final String KEY_FIELDS_ORDERING_1ST_DELIMETER = "(";
	private static final String KEY_FIELDS_ORDERING_2ND_DELIMETER = ")";	
	
	static Log logger = LogFactory.getLog(ExtSort.class);

	/**
     * Constructor for the Sort object
     * 
     * @param id
     *            Description of the Parameter
     * @param sortKeysNames
     *            Description of the Parameter
     * @param sortOrder
     *            Description of the Parameter
     */
    public ExtSort(String id, String[] sortKeys, boolean oldAscendingOrder) {
        super(id);

        this.sortKeysNames = sortKeys;
        this.sortOrderings = new boolean[sortKeysNames.length];
        Arrays.fill(sortOrderings, oldAscendingOrder);
        
        Pattern pat = Pattern.compile("^(.*)\\((.*)\\)$");
        
        for (int i = 0; i < sortKeys.length; i++) {
        	Matcher matcher = pat.matcher(sortKeys[i]);
        	if (matcher.find()) {
	        	String keyPart = sortKeys[i].substring(matcher.start(1), matcher.end(1));
	        	if (matcher.groupCount() > 1) {
	        		sortOrderings[i] = (sortKeys[i].substring(matcher.start(2), matcher.end(2))).matches("^[Aa].*");	        		
	        	}
	        	sortKeys[i] = keyPart;
        	}
        }
        
        this.numberOfTapes = DEFAULT_NUMBER_OF_TAPES;
        internalBufferCapacity=-1;
    }

    /**
     * Constructor for the Sort object
     * 
     * @param id
     *            Description of the Parameter
     * @param sortKeysNames
     *            Description of the Parameter
     */
/*    public ExtSort(String id, String[] sortKeys) {
        this(id, sortKeys);//, new SortOrder(new boolean[] { DEFAULT_ASCENDING_SORT_ORDER }));
    }*/


    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    		sorter.reset();
    	}
    }
    
    @Override
    public Result execute() throws Exception {
        
        inPort = getInputPort(READ_FROM_PORT);
        inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
        inRecord.init();
        DataRecord tmpRecord = inRecord;
         
        while (tmpRecord != null && runIt) {
			tmpRecord = inPort.readRecord(inRecord);
			if (tmpRecord != null) {
				sorter.put(inRecord);
			}
			SynchronizeUtils.cloverYield();
		}
        
        try {
			sorter.sort();
		} catch (InterruptedException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new JetelException("Error when sorting", ex);
		}
		
        while (sorter.get(recordBuffer) && runIt) {
			writeRecordBroadcastDirect(recordBuffer);
			recordBuffer.clear();
		}
		
	    broadcastEOF();
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }

    @Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		sorter.postExecute();
	}

	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException
	 *                Description of the Exception
	 * @since April 4, 2002
	 */
    @Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		try {
			// create sorter
			sorter = new ExternalSortDataRecord(getInputPort(READ_FROM_PORT).getMetadata(),
					sortKeysNames, sortOrderings, internalBufferCapacity, DEFAULT_NUMBER_OF_TAPES, localeStr, caseSensitive);
		} catch (Exception e) {
            throw new ComponentNotReadyException(e);
		}

		recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
    }

    
    @Override
    public void free() {
        if(!isInitialized()) return;
        super.free();
        if (sorter != null) {
        	try {
				sorter.free();
			} catch (InterruptedException e) {
				//DO NOTHING
			}
        }
    }
    
    /**
     * What is the capacity of internal buffer used for
     * in-memory sorting.
     * 
     * @param size buffer capacity
     */
    public void setBufferCapacity(int size){
        internalBufferCapacity = size;
    }
    
	/**
     *  Description of the Method
     *
     * @param  nodeXML  Description of Parameter
     * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
     * @since           May 21, 2002
     */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        ExtSort sort;
    	boolean oldAscendingOrder;
        if (xattribs.exists(XML_SORTORDER_ATTRIBUTE)) { 
        	// this is for backwards compatibility
        	oldAscendingOrder = xattribs.getString(XML_SORTORDER_ATTRIBUTE)
                .matches("^[Aa].*");
        } else 
        	oldAscendingOrder = true;

    	sort = new ExtSort(xattribs.getString(XML_ID_ATTRIBUTE), xattribs.getString(
                XML_SORTKEY_ATTRIBUTE).split(
                Defaults.Component.KEY_FIELDS_DELIMITER_REGEX), oldAscendingOrder);
        if (xattribs.exists(XML_SORTORDER_ATTRIBUTE)) {
        	sort.setSortOrders(xattribs.getString(XML_SORTORDER_ATTRIBUTE), sort.getSortKeyCount());
        }
        if (xattribs.exists(XML_SORTERINITIALCAPACITY_ATTRIBUTE)){
            //only for backward compatibility
            sort.setBufferCapacity(xattribs.getInteger(XML_SORTERINITIALCAPACITY_ATTRIBUTE));
        }
        if (xattribs.exists(XML_NUMBEROFTAPES_ATTRIBUTE)){
            sort.setNumberOfTapes(xattribs.getInteger(XML_NUMBEROFTAPES_ATTRIBUTE));
        }
        if (xattribs.exists(XML_BUFFER_CAPACITY_ATTRIBUTE)){
            sort.setBufferCapacity(xattribs.getInteger(XML_BUFFER_CAPACITY_ATTRIBUTE));
        }

        if (xattribs.exists(XML_LOCALE_ATTRIBUTE)) {
            sort.setLocaleStr(xattribs.getString(XML_LOCALE_ATTRIBUTE));
        }
		if (xattribs.exists(XML_CASE_SENSITIVE_ATTRIBUTE)) {
			sort.setCaseSensitive(xattribs.getBoolean(XML_CASE_SENSITIVE_ATTRIBUTE));
		}
        return sort;
    }

	private int getSortKeyCount() {
		return sortKeysNames.length;
	}

	private void setSortOrders(String string, int minLength) {
		
		String[] tmp = string.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX); 
		sortOrderings = new boolean[Math.max(tmp.length, minLength)];
		boolean lastValue = true;
		
		for (int i = 0 ; i < tmp.length ; i++) {
			lastValue = sortOrderings[i] = tmp[i].matches("^[Aa].*"); 
		}
		
		for (int i = tmp.length; i < minLength; i++) {
			sortOrderings[i] = lastValue;
		}
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
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }
        
        checkMetadata(status, getInMetadata(), getOutMetadata());

//        try {
//            init();
//        } catch (ComponentNotReadyException e) {
//            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
//            if(!StringUtils.isEmpty(e.getAttributeName())) {
//                problem.setAttributeName(e.getAttributeName());
//            }
//            status.add(problem);
//        } finally {
//        	free();
//        }
        
        return status;
    }

    private int getNumberOfTapes() {
    	return numberOfTapes;
	}

    private void setNumberOfTapes(int numberOfTapes) {
    	this.numberOfTapes = numberOfTapes;
	}

    /* (non-Javadoc)
     * @see org.jetel.graph.Node#getType()
     */
    @Override
	public String getType() {
        return COMPONENT_TYPE;
    }

	public String getLocaleStr() {
		return localeStr;
	}

	public void setLocaleStr(String localeStr) {
		this.localeStr = localeStr;
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	public void setCaseSensitive(boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}
    
	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new CopyComponentTokenTracker(this);
	}

}


/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-05  David Pavlis <david_pavlis@hotmail.com> and others.
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
 * Created on 28.6.2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.data.lookup;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.jetel.data.Defaults;
import org.jetel.data.parser.DelimitedDataParserNIO;
import org.jetel.data.parser.FixLenDataParser2;
import org.jetel.data.parser.Parser;
import org.jetel.database.DBLookupTable;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.NotFoundException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;

/**
 * @author David Pavlis
 * @since  28.6.2005
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class LookupTableFactory {

    private static final String XML_LOOKUP_TABLE_TYPE = "type";
    private static final String XML_LOOKUP_TABLE_ID = "id";
    
    private static final String XML_LOOKUP_TYPE_SIMPLE_LOOKUP = "simpleLookup";
    private static final String XML_LOOKUP_TYPE_DB_LOOKUP = "dbLookup"; 
    
    private static final String XML_LOOKUP_INITIAL_SIZE = "initialSize";
    private static final String XML_LOOKUP_KEY = "key";
    private static final String XML_METADATA_ID ="metadata";
    private static final String XML_KEY_METADATA_ID ="keyMetadata";
    private static final String XML_LOOKUP_DATA_TYPE = "dataType";
    private static final String XML_FILE_URL = "fileURL";
    private static final String XML_DATA_TYPE_DELIMITED ="delimited";
    private static final String XML_DATA_TYPE_FIXED = "fixed";
    private static final String XML_CHARSET = "charset";
    private static final String XML_SQL_QUERY = "sqlQuery";
    private static final String XML_DBCONNECTION = "dbConnection";
    
    
    public static LookupTable fromXML(TransformationGraph graph, org.w3c.dom.Node nodeXML){
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
        LookupTable lookupTable=null;
        String id;
        try {
            id = xattribs.getString(XML_LOOKUP_TABLE_ID);
        }catch(NotFoundException ex){
            throw new RuntimeException("Can't create lookup table - " + ex.getMessage());
        }
        
        if (xattribs.exists(XML_LOOKUP_TABLE_TYPE)){
            String typeStr=xattribs.getString(XML_LOOKUP_TABLE_TYPE);
            
            /******** SIMPLE LOOKUP TABLE ****************/
            if (typeStr.equalsIgnoreCase(XML_LOOKUP_TYPE_SIMPLE_LOOKUP)){
             try{
                int initialSize=xattribs.getInteger(XML_LOOKUP_INITIAL_SIZE,Defaults.Lookup.LOOKUP_INITIAL_CAPACITY);
                String[] keys=xattribs.getString(XML_LOOKUP_KEY).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
                DataRecordMetadata metadata=graph.getDataRecordMetadata(xattribs.getString(XML_METADATA_ID));
                Parser parser;
                String dataTypeStr=xattribs.getString(XML_LOOKUP_DATA_TYPE);
                
                // which data parser to use
                if (dataTypeStr.equalsIgnoreCase(XML_DATA_TYPE_DELIMITED)){
                    parser = new DelimitedDataParserNIO(xattribs.getString(XML_CHARSET,Defaults.DataParser.DEFAULT_CHARSET_DECODER));
                   
                }else{
                    parser = new FixLenDataParser2(xattribs.getString(XML_CHARSET,Defaults.DataParser.DEFAULT_CHARSET_DECODER));
                   
                }
                parser.open(new FileInputStream(xattribs.getString(XML_FILE_URL)),metadata);
                
                lookupTable=new SimpleLookupTable(id, metadata, keys, parser, initialSize);
                
             }catch(FileNotFoundException ex){
                 throw new RuntimeException("Can't create lookup table - "+ex.getMessage());
             }catch(ComponentNotReadyException ex){
                 throw new RuntimeException("Can't create lookup table - "+ex.getMessage());
             }
            /******* DATABASE LOOKUP TABLE ***********/
            }else if (typeStr.equalsIgnoreCase(XML_LOOKUP_TYPE_DB_LOOKUP)){
                String[] keys=xattribs.getString(XML_LOOKUP_KEY).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
                DataRecordMetadata metadata=graph.getDataRecordMetadata(xattribs.getString(XML_METADATA_ID));
                
                lookupTable = new DBLookupTable(id, graph.getDBConnection(xattribs.getString(XML_DBCONNECTION)),
                        	metadata, xattribs.getString(XML_SQL_QUERY));
                
                
            }else {
                throw new RuntimeException("Can't create lookup table - unknown type: "+typeStr);
            }
            
        }
        return lookupTable;
    }
    
}

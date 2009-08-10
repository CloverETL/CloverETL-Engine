
package org.jetel.data.parser;

import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TimeZone;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.IntegerDecimal;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.w3c.dom.Document;

/**
 *
 * @author Pavel Pospichal
 */
public class XPathParserWSTest extends CloverTestCase {

    private static final Logger logger = Logger.getLogger(XPathParserWSTest.class);

    private static final int PARSER_TEST_COUNT = 2;
    private static final String MAPPING_SAMPLE_PACKAGE = XPathParserWSTest.class.getPackage().getName() + ".mapping";
    private static final String XML_SAMPLE_PACKAGE = XPathParserWSTest.class.getPackage().getName() + ".xml";
    private static final String MAPPING_SAMPLE_BASENAME = "sample";
    private static final String XML_SAMPLE_BASENAME = "sample";
    private static final String CL_SOURCE_SEPARATOR = "/";

    private static String mappingSampleBaseLocation;
    private static String xmlSampleBaseLocation;

    private XPathParser[] parsers;
    private Map<Integer,ListIterator<DataRecord>>[] outputPortsRule;
    //private Map<Integer,DataRecord>[] outputPortsTestSample;
    private InputStream[] xmlDocTestSample;
    private Map<Integer, DataRecordMetadata>[] outputPortsMetadataTestSample;

    public XPathParserWSTest() {
    }

    protected void setUp() throws Exception {
    	mappingSampleBaseLocation = MAPPING_SAMPLE_PACKAGE.replace(".", CL_SOURCE_SEPARATOR);
        xmlSampleBaseLocation = XML_SAMPLE_PACKAGE.replace(".", CL_SOURCE_SEPARATOR);
    	
        parsers = new XPathParser[PARSER_TEST_COUNT];
        xmlDocTestSample = new InputStream[PARSER_TEST_COUNT];
        outputPortsRule = new Map[PARSER_TEST_COUNT];
        outputPortsMetadataTestSample = new Map[PARSER_TEST_COUNT];

        try {
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

            for (int i = 0; i < PARSER_TEST_COUNT; i++) {
            	logger.debug("Setting environment for the test: " + i);
                // mapping
                String mappingSampleLocation = constructMappingClassloaderLocation(i);

                InputStream mappingStream = ClassLoader.getSystemResourceAsStream(mappingSampleLocation);
                assertNotNull("XML mapping resource >" + mappingSampleLocation + "< not found", mappingStream);
                
                Document mapping = documentBuilder.parse(mappingStream);
                parsers[i] = new XPathParser(mapping);
                parsers[i].setDataModel(XPathParser.SupportedDataModels.W3C_XSD);
                
                // XML
                String xmlSampleLocation = constructXMLClassloaderLocation(i);
                InputStream xmlStream = ClassLoader.getSystemResourceAsStream(xmlSampleLocation);
                assertNotNull("XML data resource >" + xmlSampleLocation + "< not found", xmlStream);
                xmlDocTestSample[i] = xmlStream;

                // data record metadata
                Map<Integer, ListIterator<DataRecord>> outputPortRule = new HashMap<Integer, ListIterator<DataRecord>>();
                outputPortsRule[i] = outputPortRule;
                
                switch (i) {
                	case 0:
                		List<DataRecord> customers = createCustomerRule();
                        outputPortRule.put(0, customers.listIterator());
                		break;
                	case 1:
                		List<DataRecord> typesMix = createCloverFieldTypesMixRule();
                        outputPortRule.put(0, typesMix.listIterator());
                		break;
                }
                
                outputPortsMetadataTestSample[i] = prepareOutputPortsMetadata(outputPortRule);
            }

        } catch (Exception e) {
            logger.debug("Setting up: " + e.getMessage());
        }
    }

    public void test_getNext() {

        for (int i = 0; i < parsers.length; i++) {
        	logger.debug("Running test: " + i);
        	
            XPathParser parser = parsers[i];
            List<DataRecord> records = new LinkedList<DataRecord>();

            try {
                // init phase
                try {
                    parser.init(null);
                    parser.setDataSource(xmlDocTestSample[i]);

                    List<Integer> portIndeces = parser.getPorts();
                    for (Integer portIdex : portIndeces) {
                        DataRecord record = new DataRecord(outputPortsMetadataTestSample[i].get(portIdex));
                        record.init();
                        records.add(record);
                        parser.assignRecord(record, portIdex);
                    }

                } catch (Exception e) {
                    logger.debug("Initializing formatter:", e);
                }

                // execute phase
                try {
                    while (true) {
                        DataRecord record = parser.getNext();
                        if (record == null) {
                            break;
                        }
                        int outputPortNumber = parser.getActualPort();

                        ListIterator<DataRecord> outputPortRule = outputPortsRule[i].get(outputPortNumber);
                        assertTrue("Generated more records of port [" + outputPortNumber + "] than required.",
                                outputPortRule.hasNext());
                        assertTrue("Generated record of port [" + outputPortNumber + "] is not identical with sample.",
                                compareRecords(outputPortRule.next(), records.get(outputPortNumber)));
                        outputPortsRule[i].remove(outputPortNumber);
                    }

                } catch (Exception e) {
                    logger.debug("Executing formatter:", e);
                }

            } finally {
                // terminate phase
                try {
                    parser.close();
                } catch (Exception e) {
                    logger.debug("Terminating formatter:", e);
                }
            }

            // check if all required records were compared
            Map<Integer, ListIterator<DataRecord>> outputPortRule = outputPortsRule[i];
            for (Map.Entry<Integer, ListIterator<DataRecord>> entry : outputPortRule.entrySet()) {
                assertFalse("Not all required records for port [" + entry.getKey() + "] were generated.", entry.getValue().hasNext());
            }
        }
    }

    private static String constructMappingClassloaderLocation(int sampleNumber) {
        StringBuffer mappingClassloaderLocation = new StringBuffer();
        mappingClassloaderLocation.append(mappingSampleBaseLocation);
        mappingClassloaderLocation.append(CL_SOURCE_SEPARATOR);
        mappingClassloaderLocation.append(MAPPING_SAMPLE_BASENAME);
        mappingClassloaderLocation.append(sampleNumber);
        mappingClassloaderLocation.append(".xml");

        return mappingClassloaderLocation.toString();
    }

    private static String constructXMLClassloaderLocation(int sampleNumber) {
        StringBuffer mappingClassloaderLocation = new StringBuffer();
        mappingClassloaderLocation.append(xmlSampleBaseLocation);
        mappingClassloaderLocation.append(CL_SOURCE_SEPARATOR);
        mappingClassloaderLocation.append(XML_SAMPLE_BASENAME);
        mappingClassloaderLocation.append(sampleNumber);
        mappingClassloaderLocation.append(".xml");

        return mappingClassloaderLocation.toString();
    }

    private Map<Integer, DataRecordMetadata> prepareOutputPortsMetadata(Map<Integer, ListIterator<DataRecord>> outputPorts) {
        Map<Integer, DataRecordMetadata> outputPortsMetadata = new HashMap<Integer, DataRecordMetadata>();

        for (Map.Entry<Integer, ListIterator<DataRecord>> outputPort : outputPorts.entrySet()) {
            if (!outputPort.getValue().hasNext()) {
                throw new RuntimeException("No records defined for port "+outputPort.getKey());
            }
            DataRecordMetadata metadata = outputPort.getValue().next().getMetadata();
            outputPort.getValue().previous();
            outputPortsMetadata.put(outputPort.getKey(), metadata);
        }

        return outputPortsMetadata;
    }

    private boolean compareRecords(DataRecord sampleRecord, DataRecord generatedRecord) {
        boolean identical = true;
        logger.debug("sampleRecord:"+sampleRecord);
        logger.debug("generatedRecord:"+generatedRecord);
        
        for (int i = 0; i < sampleRecord.getNumFields(); i++) {
            if (!sampleRecord.getField(i).equals(generatedRecord.getField(i))) {
                if (sampleRecord.getField(i).isNull() && generatedRecord.getField(i).isNull()) {
                    continue;
                }
                logger.warn("sampleField \""+sampleRecord.getField(i)+"\" not equals generated field \""+generatedRecord.getField(i)+"\"");
                logger.warn("sampleField:"+((Date)((DateDataField)sampleRecord.getField(i)).getValue()).getTime());
                logger.warn("generatedField:"+((Date)((DateDataField)generatedRecord.getField(i)).getValue()).getTime());
                identical = false;
                break;
            }
        }

        return identical;
    }

    private List<DataRecord> createCustomerRule() {
        List<DataRecord> records = new LinkedList<DataRecord>();

        // data record metadata definition
        DataRecordMetadata recordMetadata = new DataRecordMetadata("Customer", DataRecordMetadata.DELIMITED_RECORD);

        // data field definition
        Map<String, Object[]> recordContent = new LinkedHashMap<String, Object[]>();
        recordContent.put("CUSTOMERID", new Object[]{DataFieldMetadata.STRING_FIELD, "10"});
        recordContent.put("CompanyName", new Object[]{DataFieldMetadata.STRING_FIELD, "Alfreds Futterkiste"});
        recordContent.put("Representative", new Object[]{DataFieldMetadata.STRING_FIELD, "Maria Anders"});
        recordContent.put("JobTitle", new Object[]{DataFieldMetadata.STRING_FIELD, "Sales Representative"});
        recordContent.put("Address", new Object[]{DataFieldMetadata.STRING_FIELD, "Obere Str. 57"});
        recordContent.put("City", new Object[]{DataFieldMetadata.STRING_FIELD, "Berlin"});
        recordContent.put("State", new Object[]{DataFieldMetadata.STRING_FIELD, ""});
        recordContent.put("PostCode", new Object[]{DataFieldMetadata.STRING_FIELD, "12209"});
        recordContent.put("Country", new Object[]{DataFieldMetadata.STRING_FIELD, "Germany"});
        recordContent.put("Phone", new Object[]{DataFieldMetadata.STRING_FIELD, "030-0074321"});
        recordContent.put("Fax", new Object[]{DataFieldMetadata.STRING_FIELD, "030-0076545"});

        for (Map.Entry<String, Object[]> entry : recordContent.entrySet()) {
            recordMetadata.addField(new DataFieldMetadata(entry.getKey(), (Character) (entry.getValue()[0]), ""));
        }

        // data field insertion
        DataRecord record = new DataRecord(recordMetadata);
        records.add(record);
        record.init();

        for (Map.Entry<String, Object[]> entry : recordContent.entrySet()) {
            record.getField(entry.getKey()).setValue(entry.getValue()[1]);
        }

        return records;
    }
    
    private List<DataRecord> createCloverFieldTypesMixRule() {
        List<DataRecord> records = new LinkedList<DataRecord>();

     // data record metadata definition
        DataRecordMetadata recordMetadata = new DataRecordMetadata("typeMix", DataRecordMetadata.DELIMITED_RECORD);

        // data field definition
        // TODO: there should be some way how to test border values of the particular data types
        Map<String,Object[]> recordContent = new LinkedHashMap<String, Object[]>();
        recordContent.put("BOOLEANTYPE", new Object[]{DataFieldMetadata.BOOLEAN_FIELD, Boolean.TRUE});
        recordContent.put("BYTETYPE", new Object[]{DataFieldMetadata.BYTE_FIELD, new byte[]{0x10, 0x13}});
        recordContent.put("BYTECOMPRESSEDTYPE", new Object[]{DataFieldMetadata.BYTE_FIELD_COMPRESSED, new byte[]{0x10, 0x13}});
        Calendar dateRule = GregorianCalendar.getInstance();
        dateRule.set(2009, 8, 4, 0, 0, 0);
        dateRule.setTimeZone(TimeZone.getTimeZone("GMT+02:00"));
        dateRule.set(Calendar.MILLISECOND, 0);
        
        recordContent.put("DATETYPE", new Object[]{DataFieldMetadata.DATE_FIELD, dateRule.getTime()});
        //recordContent.put("DATETIMETYPE", new Object[]{DataFieldMetadata.DATETIME_FIELD, GregorianCalendar.getInstance().getTime()});
        Decimal decimalValue = new IntegerDecimal(10, 3);
        decimalValue.setValue(1234L);
        recordContent.put("DECIMALTYPE", new Object[]{DataFieldMetadata.DECIMAL_FIELD, decimalValue});
        recordContent.put("INTEGERTYPE", new Object[]{DataFieldMetadata.INTEGER_FIELD, new Integer(1234)});
        recordContent.put("LONGTYPE", new Object[]{DataFieldMetadata.LONG_FIELD, new Long(-1234L)});
        recordContent.put("NUMERICTYPE", new Object[]{DataFieldMetadata.NUMERIC_FIELD, Long.MAX_VALUE});
        recordContent.put("STRINGTYPE", new Object[]{DataFieldMetadata.STRING_FIELD, new String("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam iaculis lorem at dolor tempus vel vehicula erat scelerisque.")});

        for (Map.Entry<String, Object[]> entry : recordContent.entrySet()) {
            recordMetadata.addField(new DataFieldMetadata(entry.getKey(), (Character) (entry.getValue()[0]), ""));
        }

        // data field insertion
        DataRecord record = new DataRecord(recordMetadata);
        records.add(record);
        record.init();

        for (Map.Entry<String, Object[]> entry : recordContent.entrySet()) {
            record.getField(entry.getKey()).setValue(entry.getValue()[1]);
        }

        return records;
    }

}
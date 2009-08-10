
package org.jetel.data.formatter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.io.SAXReader;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.IntegerDecimal;
import org.jetel.graph.extension.PortDefinition;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.sun.tools.javac.comp.Todo;

/**
 *
 * @author Pavel Pospichal
 */
public class XmlTemplateFormatterWSTest extends CloverTestCase {

    private static final Logger logger = Logger.getLogger(XmlTemplateFormatterWSTest.class);

    private static final int FORMATTER_TEST_COUNT = 2;
    private static final String MAPPING_SAMPLE_PACKAGE = XmlTemplateFormatterWSTest.class.getPackage().getName() + ".mapping";
    private static final String XSD_SAMPLE_PACKAGE = XmlTemplateFormatterWSTest.class.getPackage().getName() + ".xsd";
    private static final String MAPPING_XML_BASENAME = "sample";
    private static final String CL_SOURCE_SEPARATOR = "/";
    private static final String OUTPUT_CHARSET_DEFAULT = "UTF-8";
    
    private static String mappingSampleBaseLocation;
    private static String xsdSampleBaseLocation;

    private List<XmlTemplateFormatter> formatters;
    private List<Map<Integer, List<DataRecord>>> inputPortsTestSample;
    private static List<String>[] XSDNamesTestSample;
    private static List<Source>[] XSDTestSample;


    public XmlTemplateFormatterWSTest() {
    }
    
    @Override
    protected void setUp() throws Exception {
    	Defaults.init("");
        mappingSampleBaseLocation = MAPPING_SAMPLE_PACKAGE.replace(".", CL_SOURCE_SEPARATOR);
        xsdSampleBaseLocation = XSD_SAMPLE_PACKAGE.replace(".", CL_SOURCE_SEPARATOR);
        
        XSDNamesTestSample = new List[FORMATTER_TEST_COUNT];
        // sample0
        List<String> xsdDocNames = new LinkedList<String>();
        XSDNamesTestSample[0] = xsdDocNames;
        xsdDocNames.add("sampleRule01.xsd");
        xsdDocNames.add("sampleRule00.xsd");
        // sample1
        xsdDocNames = new LinkedList<String>();
        XSDNamesTestSample[1] = xsdDocNames;
        xsdDocNames.add("sampleRule10.xsd");
        
        formatters = new LinkedList<XmlTemplateFormatter>();
        XSDTestSample = new List[FORMATTER_TEST_COUNT];

        try {
            
            for (int i = 0; i < FORMATTER_TEST_COUNT; i++) {
				logger.debug("Setting environment for the test: " + i);
                String mappingXmlLocation = constructMappingClassloaderLocation(i);

                Document mappingXml = createMapping(mappingXmlLocation);

                XmlTemplateFormatter formatter = new XmlTemplateFormatter(mappingXml);
                formatters.add(formatter);
                logger.debug("Created formatter with the mapping definition at [" + mappingXmlLocation + "]");
                // XSD
                List<Source> xsdDocs = new LinkedList<Source>();
                XSDTestSample[i] = xsdDocs;
                for (String xsdDocName : XSDNamesTestSample[i]) {
                    InputStream xsdStream = ClassLoader.getSystemResourceAsStream(constructXSDClassloaderLocation(xsdDocName));
                    assertNotNull("XSD resource >" + xsdDocName + "< not found", xsdStream);
                    
                    xsdDocs.add(new StreamSource(xsdStream));
                    logger.debug("XSD resource >" + xsdDocName + "< found");
                }
            }

            inputPortsTestSample = new LinkedList<Map<Integer, List<DataRecord>>>();
            Map inputPortsSample;
            // sample0
            inputPortsSample = new HashMap<Integer, List<DataRecord>>();
            inputPortsTestSample.add(inputPortsSample);

            inputPortsSample.put(0, createCustomerSample());
            // sample1
            inputPortsSample = new HashMap<Integer, List<DataRecord>>();
            inputPortsTestSample.add(inputPortsSample);

            inputPortsSample.put(0, createCloverFieldTypesMixSample());
        } catch (Exception e) {
            logger.debug("Setting up: " + e.getMessage());
        }
    }

	public void test_write() {
        //logger.debug("formatters:" + formatters.size());

        for (XmlTemplateFormatter formatter : formatters) {
        	logger.debug("Running test: " + formatters.indexOf(formatter));
        	
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            
            try {
                Map<Integer, List<DataRecord>> inputPorts = inputPortsTestSample.get(formatters.indexOf(formatter));

                // init phase
                try {
                    formatter.setDataTarget(output);
                    formatter.setUseRootElement(false);
                    formatter.setCharset(OUTPUT_CHARSET_DEFAULT);
                    formatter.init(prepareDataRecordMetadatas(inputPorts));
                } catch (Exception e) {
                    logger.debug("Initializing formatter:", e);
                }

                // execute phase
                try {
                    formatter.write(preparePortDefinitions(inputPorts));
                } catch (Exception e) {
                    logger.debug("Executing formatter:", e);
                }

            } finally {
                // terminate phase
                try {
                    formatter.finish();
                    formatter.close();
                } catch (Exception e) {
                    logger.debug("Terminating formatter:", e);
                }

            }

            logger.debug("XML:"+output.toString());

            // validate generated XML document
            try {
                assertTrue("Generated XML document is empty.", output.size() != 0);

                Source xmlDoc = new StreamSource(new ByteArrayInputStream(output.toByteArray()));
                boolean valid = validateXML(xmlDoc, XSDTestSample[formatters.indexOf(formatter)]);
                assertTrue("XML document is not valid.", valid);
            } catch(Exception e) {
                logger.debug(e);
            }
            
        }
    }

    private static Document createMapping(String mappingXmlLocation) throws Exception {
        InputStream mappingStream = ClassLoader.getSystemResourceAsStream(mappingXmlLocation);
        assertNotNull("XML mapping resource >" + mappingXmlLocation + "< not found", mappingStream);
        SAXReader reader = new SAXReader();
        reader.setIgnoreComments(true);
        reader.setIncludeExternalDTDDeclarations(false);
        reader.setMergeAdjacentText(false);
        reader.setStripWhitespaceText(true);

        Document doc = reader.read(mappingStream);

        return doc;
    }

    private static String constructMappingClassloaderLocation(int sampleIndex) {
        StringBuffer mappingClassloaderLocation = new StringBuffer();
        mappingClassloaderLocation.append(mappingSampleBaseLocation);
        mappingClassloaderLocation.append(CL_SOURCE_SEPARATOR);
        mappingClassloaderLocation.append(MAPPING_XML_BASENAME);
        mappingClassloaderLocation.append(sampleIndex);
        mappingClassloaderLocation.append(".xml");

        return mappingClassloaderLocation.toString();
    }

    private static String constructXSDClassloaderLocation(String xsdName) {
        StringBuffer xsdClassloaderLocation = new StringBuffer();
        xsdClassloaderLocation.append(xsdSampleBaseLocation);
        xsdClassloaderLocation.append(CL_SOURCE_SEPARATOR);
        xsdClassloaderLocation.append(xsdName);

        return xsdClassloaderLocation.toString();
    }

    private List<DataRecordMetadata> prepareDataRecordMetadatas(Map<Integer, List<DataRecord>> inputPorts) {
        List<DataRecordMetadata> dataRecordMetadatas = new LinkedList<DataRecordMetadata>();

        for (List<DataRecord> records : inputPorts.values()) {
            if (records.isEmpty()) continue;
            DataRecordMetadata recordMetadata = records.get(0).getMetadata();
            dataRecordMetadatas.add(recordMetadata);
        }
        return dataRecordMetadatas;
    }

    private List<PortDefinition> preparePortDefinitions(Map<Integer, List<DataRecord>> inputPorts) {
        List<PortDefinition> portDefinitions = new LinkedList<PortDefinition>();
        
        for (Map.Entry<Integer, List<DataRecord>> entry : inputPorts.entrySet()) {
            PortDefinition portDefinition = new PortDefinition();
            portDefinitions.add(portDefinition);
            
            portDefinition.dataRecords = entry.getValue();
            portDefinition.portIndex = entry.getKey();
            if (entry.getValue().isEmpty()) {
                throw new RuntimeException("No records defined for port "+entry.getKey());
            }
            DataRecordMetadata recordMetadata = entry.getValue().get(0).getMetadata();
            portDefinition.setMetadata(recordMetadata);
        }

        return portDefinitions;
    }

    private List<DataRecord> createCustomerSample() {
        List<DataRecord> records = new LinkedList<DataRecord>();

        // data record metadata definition
        DataRecordMetadata recordMetadata = new DataRecordMetadata("Customer", DataRecordMetadata.DELIMITED_RECORD);

        // data field definition
        Map<String,Object[]> recordContent = new LinkedHashMap<String, Object[]>();
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

        for (Map.Entry<String, Object[]> entry: recordContent.entrySet()) {
            recordMetadata.addField(new DataFieldMetadata(entry.getKey(), (Character)(entry.getValue()[0]),""));
        }

        // data field insertion
        DataRecord record = new DataRecord(recordMetadata);
        records.add(record);
        record.init();

        for (Map.Entry<String, Object[]> entry: recordContent.entrySet()) {
            record.getField(entry.getKey()).setValue(entry.getValue()[1]);
        }
        
        return records;
    }
    
    private List<DataRecord> createCloverFieldTypesMixSample() {
        List<DataRecord> records = new LinkedList<DataRecord>();

        // data record metadata definition
        DataRecordMetadata recordMetadata = new DataRecordMetadata("typeMix", DataRecordMetadata.DELIMITED_RECORD);

        // data field definition
        // TODO: there should be some way how to test border values of the particular data types
        Map<String,Object[]> recordContent = new LinkedHashMap<String, Object[]>();
        recordContent.put("BOOLEANTYPE", new Object[]{DataFieldMetadata.BOOLEAN_FIELD, Boolean.TRUE});
        recordContent.put("BYTETYPE", new Object[]{DataFieldMetadata.BYTE_FIELD, new byte[]{0x10, 0x13}});
        recordContent.put("BYTECOMPRESSEDTYPE", new Object[]{DataFieldMetadata.BYTE_FIELD_COMPRESSED, new byte[]{0x10, 0x13}});
        Calendar dateSample = GregorianCalendar.getInstance();
        dateSample.set(2009, 8, 4, 0, 0, 0);
        
        recordContent.put("DATETYPE", new Object[]{DataFieldMetadata.DATE_FIELD, dateSample.getTime()});
        //recordContent.put("DATETIMETYPE", new Object[]{DataFieldMetadata.DATETIME_FIELD, GregorianCalendar.getInstance().getTime()});
        Decimal decimalValue = new IntegerDecimal(10, 3);
        decimalValue.setValue(1234L);
        recordContent.put("DECIMALTYPE", new Object[]{DataFieldMetadata.DECIMAL_FIELD, decimalValue});
        recordContent.put("INTEGERTYPE", new Object[]{DataFieldMetadata.INTEGER_FIELD, new Integer(1234)});
        recordContent.put("LONGTYPE", new Object[]{DataFieldMetadata.LONG_FIELD, new Long(-1234L)});
        recordContent.put("NUMERICTYPE", new Object[]{DataFieldMetadata.NUMERIC_FIELD, Long.MAX_VALUE});
        recordContent.put("STRINGTYPE", new Object[]{DataFieldMetadata.STRING_FIELD, new String("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam iaculis lorem at dolor tempus vel vehicula erat scelerisque.")});
        
        for (Map.Entry<String, Object[]> entry: recordContent.entrySet()) {
            recordMetadata.addField(new DataFieldMetadata(entry.getKey(), (Character)(entry.getValue()[0]),""));
        }

        // data field insertion
        DataRecord record = new DataRecord(recordMetadata);
        records.add(record);
        record.init();

        for (Map.Entry<String, Object[]> entry: recordContent.entrySet()) {
            record.getField(entry.getKey()).setValue(entry.getValue()[1]);
        }
        
        return records;
    }

    private boolean validateXML(Source xmlDocument, List<Source> schemas) {
        boolean isValid = true;

        try {
            // validates xml content against W3C XSD schema language
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

            XMLErrorHandler handler = new XMLErrorHandler();

            // XML Schema prepared
            Schema schema = schemaFactory.newSchema(schemas.toArray(new Source[schemas.size()]));

            Validator validator = schema.newValidator();

            validator.setErrorHandler(handler);

            validator.validate(xmlDocument);

            isValid = handler.isIsValid();

        } catch (Exception e) {
            logger.error("Validating XML document.",e);
            isValid = false;
        }

        return isValid;
    }

    private static class XMLErrorHandler implements ErrorHandler {
        private boolean valid = true;

        public boolean isIsValid() {
            return valid;
        }
        
        public void warning(SAXParseException saxe) throws SAXException {
            logger.warn("Warning during XML document validation.\nReason :"+saxe.getMessage());
            // warnings are not important
        }

        public void error(SAXParseException saxe) throws SAXException {
            logger.warn("Error during XML document validation.\nReason :"+saxe.getMessage());
            valid = false;
        }

        public void fatalError(SAXParseException saxe) throws SAXException {
            logger.warn("Fatal error during XML document validation.\nReason :"+saxe.getMessage());
            valid = false;
        }

    }
}
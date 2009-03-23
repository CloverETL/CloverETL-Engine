package org.jetel.util;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.test.CloverTestCase;
import org.jetel.util.ddl2clover.DDL2Clover;
import org.jetel.util.ddl2clover.ParseException;
import org.jetel.util.file.FileUtils;

public class DDL2CloverTest extends CloverTestCase {
	
	List<DataRecordMetadata> ddlMetadata = new ArrayList<DataRecordMetadata>(6);
	List<DataRecordMetadata> xmlMetadata = new ArrayList<DataRecordMetadata>(6);
	
	DDL2Clover ddl2Clover;
	
	private final static String[] metadataFile = {"test/pet.fmt", "test/pet2.fmt", "test/zamestnanci.fmt",
		"test/proj_zam.fmt", "test/person.fmt", "test/employee.fmt"
	};
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
		DataRecordMetadataXMLReaderWriter metaReader = new DataRecordMetadataXMLReaderWriter();
		for (int i = 0; i < metadataFile.length; i++) {
			xmlMetadata.add(metaReader.read(FileUtils.getInputStream(null, metadataFile[i])));
		}
		ddl2Clover = new DDL2Clover(FileUtils.getInputStream(null, "test/ddl_statements.txt"));
	}

	public void testGetDataRecordMetadataList() {
		try {
			ddlMetadata = ddl2Clover.getDataRecordMetadataList();
		} catch (ParseException e) {
			fail(e.getMessage());
		}
		if (ddlMetadata.size() != xmlMetadata.size()) {
			fail("Number of parsed metadata: " + ddlMetadata.size() + ", should be: " +xmlMetadata.size());
		}
		for (int i = 0; i < ddlMetadata.size(); i++) {
			if (!xmlMetadata.get(i).equals(ddlMetadata.get(i))) {
				DataRecordMetadata xmlMeta = xmlMetadata.get(i);
				DataRecordMetadata ddlMeta = ddlMetadata.get(i);
				int j = 0;
				for (j = 0; j < xmlMeta.getNumFields(); j++) {
					if (j >= ddlMeta.getNumFields()) {
						fail("Different number of fields: xml metadata - " + xmlMeta.getNumFields() + ", ddl metadata - "
								+ ddlMeta.getNumFields());
					}
					assertEquals("Error on metadata no " +  i + ", field no " + j, xmlMeta.getField(j), ddlMeta.getField(j));
				}
				if (j < ddlMeta.getNumFields()) {
					fail("Different number of fields: xml metadata - " + xmlMeta.getNumFields() + ", ddl metadata - "
							+ ddlMeta.getNumFields());
				}
			}
		}
		DataRecordMetadata meta = ddlMetadata.get(2);
		assertFalse(meta.getField("OsobniCislo").isNullable());
		meta = ddlMetadata.get(3);
		assertFalse(meta.getField("ID_Projektu").isNullable());
		meta = ddlMetadata.get(5);
		assertTrue(meta.getField("Title").isNullable());
		assertEquals("crew", meta.getField("Title").getDefaultValueStr());
	}

}

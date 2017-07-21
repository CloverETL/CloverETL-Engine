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
package org.jetel.hadoop.test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.hadoop.component.IHadoopSequenceFileFormatter;
import org.jetel.hadoop.component.IHadoopSequenceFileParser;
import org.jetel.hadoop.connection.HadoopFileStatus;
import org.jetel.hadoop.connection.IHadoopConnection;
import org.jetel.hadoop.connection.IHadoopInputStream;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.junit.After;
import org.junit.Before;

public class TestHadoopConnection {

    private static final String PLUGINS_KEY = "cloveretl.plugins";

    private static final String PLUGINS_DEFAULT_DIR = "..";

    private DataRecordMetadata metadata;
    private DataRecord record;

    protected void initEngine() {
	initEngine(null);

    }

    protected void initEngine(String defaultPropertiesFile) {
	final String pluginsDir;

	final String pluginsProperty = System.getenv(PLUGINS_KEY);
	if (pluginsProperty != null) {
	    pluginsDir = pluginsProperty;
	}
	else {
	    pluginsDir = PLUGINS_DEFAULT_DIR;
	}

	System.out.println("Cloveretl plugins: " + pluginsDir);
	EngineInitializer.initEngine(pluginsDir, defaultPropertiesFile, null);
	EngineInitializer.forceActivateAllPlugins();
    }

    @Before
    public void setUp() throws Exception {
	initEngine();
	metadata = new DataRecordMetadata("test");
	metadata.addField(new DataFieldMetadata("key", DataFieldType.STRING, (short) 20));
	metadata.addField(new DataFieldMetadata("value", DataFieldType.STRING, (short) 20));
	record = new DataRecord(metadata);
	record.init();
	record.getField("key").fromString("1");
	record.getField("value").fromString("1");
    }

    @After
    public void tearDown() throws Exception {
    }

    private static void printDir(IHadoopConnection conn, HadoopFileStatus status[]) throws IOException {
	for (int i = 0; i < status.length; i++) {
	    URI file = status[i].getFile();

	    System.out.println(String.format("%s :size %d : date %tT-%TD", file.getPath(),
		    status[i].getSize(), status[i].getModificationTime(), status[i].getModificationTime()));
	    if (status[i].isDir()) {
		printDir(conn, conn.listStatus(status[i].getFile()));
	    }
	}

    }

//    @Test
//    public void testGetConnection() {
//
//	HadoopConnection conn = new HadoopConnection("ABC", "192.168.1.184", "8020", null, null, false,
//	/*
//	 * "file:/Users/dpavlis/Documents/eclipse/workspace_trunk/cloveretl.component.hadooploader/lib/hadoop-core.jar"
//	 * ,
//	 */
//	"file:/Users/dpavlis/Documents/eclipse/workspace_trunk/HadoopTest/lib/hadoop-core-0.20.2-cdh3u2.jar",
//		null);
//
//	try {
////	    conn.setHadoopModuleImplementationPath(new URL(
////		    "file:/Users/dpavlis/Documents/eclipse/workspace_trunk/cloveretl.component.hadoop/lib/cloveretl.component.hadooploader.jar"));
//	    conn.init();
//	} catch (ComponentNotReadyException e) {
//	    // TODO Auto-generated catch block
//	    e.printStackTrace();
////	} catch (MalformedURLException e) {
////	    // TODO Auto-generated catch block
////	    e.printStackTrace();
//	}
//	try {
//	    IHadoopConnection mycon = conn.getConnection();
//
//	    System.out.println(mycon.toString());
//
//	    printDir(mycon, mycon.listStatus(new URI("/")));
//	    readFile(mycon);
//
//	    System.out.println("---- creating file -----");
//	    crateFile(mycon);
//	    System.out.println("---- reading file -----");
//	    readStructuredFile(mycon);
//
//	} catch (ComponentNotReadyException e) {
//	    // TODO Auto-generated catch block
//	    e.printStackTrace();
//	} catch (IOException e) {
//	    // TODO Auto-generated catch block
//	    e.printStackTrace();
//	} catch (URISyntaxException e) {
//	    // TODO Auto-generated catch block
//	    e.printStackTrace();
//	} catch (Exception e) {
//	    e.printStackTrace();
//	}
//
//    }

    public static void readFile(IHadoopConnection conn) {

	IHadoopInputStream in = null;
	try {
	    in = conn.open(new URI("/user/hive/warehouse/sample_07/sample_07.csv"));
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (URISyntaxException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	try {
	    String ln;
	    while ((ln = in.readLine()) != null) {
		System.out.println(ln);
	    }

	    in.close();

	} catch (IOException ex) {
	    ex.printStackTrace();
	}
    }

    public void crateFile(IHadoopConnection conn) throws IOException, ComponentNotReadyException {
	IHadoopSequenceFileFormatter formatter = conn.createFormatter("key", "value", true);
	formatter.init(metadata);
	formatter.setDataTarget(new File("/user/dpavlis/mytest2.dat"));
	for (int i = 0; i < 100; i++) {
	    record.getField(1).fromString("" + i);
	    formatter.write(record);
	}
	formatter.close();
    }

    public void readStructuredFile(IHadoopConnection conn) throws JetelException, IOException,
	    ComponentNotReadyException {
	IHadoopSequenceFileParser reader = conn.createParser("key", "value", metadata);
	reader.setDataSource(new File("/user/dpavlis/mytest2.dat"));
	reader.init();

	for (int i = 0; i < 100; i++) {
	    reader.getNext(record);
	    System.out.println(record.toString());
	}
	reader.free();
    }

}

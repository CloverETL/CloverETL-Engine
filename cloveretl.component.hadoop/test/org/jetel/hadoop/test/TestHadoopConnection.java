package org.jetel.hadoop.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.Properties;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.hadoop.connection.*;


public class TestHadoopConnection {

	private static final String PLUGINS_KEY = "cloveretl.plugins";

	private static final String PLUGINS_DEFAULT_DIR = "..";
	
	protected void initEngine() {
		initEngine(null);
	}
	
	protected void initEngine(String defaultPropertiesFile) {
		final String pluginsDir;

		final String pluginsProperty = System.getenv(PLUGINS_KEY);
		if (pluginsProperty != null) {
			pluginsDir = pluginsProperty;
		} else {
			pluginsDir = PLUGINS_DEFAULT_DIR;
		}

		System.out.println("Cloveretl plugins: " + pluginsDir);
		EngineInitializer.initEngine(pluginsDir, defaultPropertiesFile, null);
		EngineInitializer.forceActivateAllPlugins();
	}

	
	@Before
	public void setUp() throws Exception {
		initEngine();
		
	}

	@After
	public void tearDown() throws Exception {
	}

	
	private static void printDir(IHadoopConnection conn,HadoopFileStatus status[]) throws IOException{
        for (int i=0;i<status.length;i++){
        	URI file = status[i].getFile();
        	
        	System.out.println(String.format("%s :size %d : date %tT-%TD",file.getPath(),status[i].getSize(), status[i].getModificationTime(),status[i].getModificationTime()));
        	if (status[i].isDir()){
        		printDir(conn,conn.listStatus(status[i].getFile()));
        	}
        }
		
	}
	
	@Test
	public void testGetConnection() {
		
		HadoopConnection conn = new HadoopConnection("ABC", "192.168.1.184", "8020", null, null, false,
				/*"file:/Users/dpavlis/Documents/eclipse/workspace_trunk/cloveretl.component.hadooploader/lib/hadoop-core.jar",
				 * 
				 *  
				 */
				"file:/Users/dpavlis/Documents/eclipse/workspace_trunk/HadoopTest/lib/hadoop-core-0.20.2-cdh3u2.jar"
				,null);
		
		try {
			conn.setHadoopModuleImplementationPath(new URL("file:/Users/dpavlis/Documents/eclipse/workspace_trunk/cloveretl.component.hadoop/lib/cloveretl.component.hadooploader.jar"));
			conn.init();
		} catch (ComponentNotReadyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			IHadoopConnection mycon=conn.getConnection();
			
			System.out.println(mycon.toString());
			
			printDir(mycon, mycon.listStatus(new URI("/")));
			readFile(mycon);
			
			
			
			
		} catch (ComponentNotReadyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	public static void readFile(IHadoopConnection conn){
		
		IHadoopInputStream in=null;
		try {
			in = conn.open(new URI("/user/hive/warehouse/sample_07/sample_07.csv"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try{
		String ln;
		while( (ln=in.readLine())!=null){
			System.out.println(ln);
		}
			
		
		in.close();
		
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
	
	

}

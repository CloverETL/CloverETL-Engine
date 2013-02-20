/**
 * 
 */
package org.jetel.hadoop.test;

import static org.junit.Assert.*;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.mapred.JobClient;
import org.jetel.hadoop.provider.mapreduce.HadoopConnectingMapReduceProvider;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 19.12.2012
 */
public class HadoopConnectingMapReduceProviderTest {

	private HadoopConnectingMapReduceProvider mr;
	private JobClientMock jc;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		mr = new HadoopConnectingMapReduceProvider();
		//jc = new JobClientMock();
		connect();
	}

	/**
	 * Test method for {@link HadoopConnectingMapReduceProvider#connect(HadoopMapReduceConnectionData, Properties)}.
	 * @throws IllegalAccessException 
	 */
	@Test
	public void testConnect() throws IllegalAccessException {
		disconnect();
		try{
			mr.connect(null, new Properties());
			fail("Did not throw for null data");
		} catch (Exception e) {
		}
	}

	/**
	 * Test method for {@link HadoopConnectingMapReduceProvider#close()}.
	 * @throws Exception 
	 */
	@Test
	public void testClose() throws Exception {
		disconnect();
		try{
			mr.close();
		} catch (IOException e) {
			fail("Calling close on disconnected mapred provider shoul not throw");
		}
		setUp();
		mr.close();
		assertEquals("Did not close job client exactly once.", 1, jc.getCloseCallsCount());
		assertNull("Did not release job client", RefUtil.getForType(mr, JobClient.class));
		setUp();
		jc.setCloseThrows(true);
		assertEquals("Did not close job client exactly once.", 1, jc.getCloseCallsCount());
		assertNull("Did not release job client", RefUtil.getForType(mr, JobClient.class));
	}

	/**
	 * Test method for {@link HadoopConnectingMapReduceProvider#sendJob(HadoopMapReduceJob, Properties)}.
	 */
	@Test
	public void testSendJob() {
		try{
			mr.sendJob(null, new Properties());
			fail("Null job did not throw");
		} catch (Exception e) {
		}
		
	}


	protected void connect() throws IllegalAccessException{
		RefUtil.setForType(mr, jc, JobClient.class);
	}
	
	protected void disconnect() throws IllegalAccessException{
		RefUtil.setForType(mr, (JobClient) null, JobClient.class);
	}
}

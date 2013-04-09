/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Jan 24, 2005
 *  Copyright (C) 2002-2005  David Pavlis, Wes Maciorowski
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.jetel.graph;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;

public class EdgeTest extends CloverTestCase {
	private DataRecordMetadata aDelimitedDataRecordMetadata;
	private EdgeBase edge;
	
	public EdgeTest(String name){
	    super(name);
	}
	
	@Override
	protected void setUp() throws Exception { 
		super.setUp();
	    
		aDelimitedDataRecordMetadata = new DataRecordMetadata("record2",DataRecordMetadata.DELIMITED_RECORD);
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field1",DataFieldMetadata.STRING_FIELD,":"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field3",DataFieldMetadata.INTEGER_FIELD,(short)23));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field4",DataFieldMetadata.INTEGER_FIELD,";"));

	}

	@Override
	protected void tearDown() {
		aDelimitedDataRecordMetadata = null;
		edge=null;
	}


	/**
	 *  Test for @link org.jetel.graph.DirectEdge
	 *
	 */
	public void test_1_sendData() throws IOException, InterruptedException {
	    final int NUM_REC=1;
	    DataRecord record=DataRecordFactory.newRecord(aDelimitedDataRecordMetadata);
	    record.init();
	    
		edge=new DirectEdgeFastPropagate(new Edge("testEdge",aDelimitedDataRecordMetadata));
		
	    edge.init();

		assertTrue(!edge.isEOF());
		assertFalse(edge.hasData());
	
		for (int i=0;i<NUM_REC;i++){
		    edge.writeRecord(record);
		}
		assertTrue(edge.hasData());
	    assertNotNull(edge.readRecord(record));
	}
	
	public void test_2_sendData() throws InterruptedException, IOException {
	    ProducerThread thread1;
	    ConsumerThread thread2;
	    
		edge=new DirectEdge(new Edge("testEdge",aDelimitedDataRecordMetadata));
		
	    edge.init();

		final DataRecord record1=DataRecordFactory.newRecord(aDelimitedDataRecordMetadata);
	    final DataRecord record2=DataRecordFactory.newRecord(aDelimitedDataRecordMetadata);
	    record2.init();
	    record1.init();
	    assertTrue(!edge.isEOF());
		assertFalse(edge.hasData());
		
		thread1=new ProducerThread(record1,(DirectEdge)edge);
		thread2=new ConsumerThread(record2,(DirectEdge)edge);
		
		// ROUND 1 - MAX, MIN priority
		thread1.setPriority(Thread.MAX_PRIORITY);
		thread2.setPriority(Thread.MIN_PRIORITY);
		thread1.start();
		thread2.start();
		thread1.join();
		thread2.join();
		
		assertEquals(ProducerThread.NUM_REC,thread2.getCounter());
		
		// ROUND 2 - MIN , MAX priority
	    assertTrue(edge.isEOF());
	    
		edge=new DirectEdge(new Edge("testEdge",aDelimitedDataRecordMetadata));
		
	    edge.init();
	    
		assertFalse(edge.isEOF());
		assertFalse(edge.hasData());
		thread1=new ProducerThread(record1,(DirectEdge)edge);
		thread2=new ConsumerThread(record2,(DirectEdge)edge);
		thread1.setPriority(Thread.MIN_PRIORITY);
		thread2.setPriority(Thread.MAX_PRIORITY);
		
		thread1.start();
		thread2.start();
		thread1.join();
		thread2.join();
		
		assertEquals(ProducerThread.NUM_REC,thread2.getCounter());
		
	}
	
	
	private static class ProducerThread extends Thread {
	    public static final int NUM_REC=999999;
	    DataRecord record;
	    DirectEdge edge;
	    
	    static final Logger log = Logger.getLogger(ProducerThread.class);
	    
	    ProducerThread(DataRecord record, DirectEdge edge){
	        this.record=record;
	        this.edge=edge;
	    }
	    
	    @Override
		public void run(){
	        for (int i=0;i<NUM_REC;i++){
	            try{
	                edge.writeRecord(record);
	                //System.out.println(">> SENT RECORD ***");
	            }catch(Exception e){
	            	log.warn("error write record", e);
	            }
	        }
	        try {
				edge.eof();
			} catch (InterruptedException e) {
            	log.warn("error eof", e);
			}
	    }
	} 
	
	private static class ConsumerThread extends Thread {
	    DataRecord record;
	    DirectEdge edge;
	    int counter;

	    static final Logger log = Logger.getLogger(ConsumerThread.class);
	    
	    ConsumerThread(DataRecord record, DirectEdge edge){
	        this.record=record;
	        this.edge=edge;
	    }
	    
	    @Override
		public void run(){
	        counter=0;
	        try{
	            while(edge.readRecord(record)!=null){
	                counter++;
	                //System.out.println("<< GOT RECORD ***");
	            }
	        }catch(Exception e){
            	log.warn("error read record", e);
	        }
	    }
	    
	    public int getCounter(){
	        return counter;
	    }
	}
}
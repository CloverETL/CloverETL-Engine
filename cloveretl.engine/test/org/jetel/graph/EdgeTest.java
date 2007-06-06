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
import junit.framework.TestCase;

import org.jetel.data.DataRecord;
import org.jetel.graph.DirectEdge;
import org.jetel.graph.Edge;
import org.jetel.main.runGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

public class EdgeTest extends TestCase {
	private DataRecordMetadata aDelimitedDataRecordMetadata;
	private EdgeBase edge;
	
	public EdgeTest(String name){
	    super(name);
	}
	
	protected void setUp() { 
		
		runGraph.initEngine(null, null);
		
		aDelimitedDataRecordMetadata = new DataRecordMetadata("record2",DataRecordMetadata.DELIMITED_RECORD);
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field0",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field1",DataFieldMetadata.STRING_FIELD,":"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field2",DataFieldMetadata.INTEGER_FIELD,";"));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field3",DataFieldMetadata.INTEGER_FIELD,(short)23));
		aDelimitedDataRecordMetadata.addField(new DataFieldMetadata("Field4",DataFieldMetadata.INTEGER_FIELD,";"));

	}

	protected void tearDown() {
		aDelimitedDataRecordMetadata = null;
		edge=null;
	}


	/**
	 *  Test for @link org.jetel.graph.DirectEdge
	 *
	 */
	public void test_1_sendData() {
	    final int NUM_REC=1;
	    DataRecord record=new DataRecord(aDelimitedDataRecordMetadata);
	    record.init();
	    
		edge=new DirectEdgeFastPropagate(new Edge("testEdge",aDelimitedDataRecordMetadata));
		
		try{
		    edge.init();
		}catch(IOException ex){
		    throw new RuntimeException(ex);
		}

		assertTrue(!edge.isEOF());
		assertFalse(edge.hasData());
	
		try{
		for (int i=0;i<NUM_REC;i++){
		    edge.writeRecord(record);
		}
		}catch(Exception ex){
		    throw new RuntimeException(ex);
		}
		assertTrue(edge.hasData());
		try{
		    assertNotNull(edge.readRecord(record));
		}catch(Exception ex){
		    throw new RuntimeException(ex);
		}
	}
	
	public void test_2_sendData() {
	    ProducerThread thread1;
	    ConsumerThread thread2;
	    
		edge=new DirectEdge(new Edge("testEdge",aDelimitedDataRecordMetadata));
		
		try{
		    edge.init();
		}catch(IOException ex){
		    throw new RuntimeException(ex);
		}

		final DataRecord record1=new DataRecord(aDelimitedDataRecordMetadata);
	    final DataRecord record2=new DataRecord(aDelimitedDataRecordMetadata);
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
		try{
		thread1.join();
		thread2.join();
		}
		catch(Exception ex){
		    
		}
		
		assertEquals(ProducerThread.NUM_REC,thread2.getCounter());
		
		// ROUND 2 - MIN , MAX priority
	    assertTrue(!edge.isEOF());
		assertFalse(edge.hasData());
		thread1=new ProducerThread(record1,(DirectEdge)edge);
		thread2=new ConsumerThread(record2,(DirectEdge)edge);
		thread1.setPriority(Thread.MIN_PRIORITY);
		thread2.setPriority(Thread.MAX_PRIORITY);
		
		thread1.start();
		thread2.start();
		try{
		thread1.join();
		thread2.join();
		}
		catch(Exception ex){
		    
		}
		
		assertEquals(ProducerThread.NUM_REC,thread2.getCounter());
		
	}
	
	
	private static class ProducerThread extends Thread {
	    public static final int NUM_REC=999999;
	    DataRecord record;
	    DirectEdge edge;
	    
	    ProducerThread(DataRecord record, DirectEdge edge){
	        this.record=record;
	        this.edge=edge;
	    }
	    
	    public void run(){
	        for (int i=0;i<NUM_REC;i++){
	            try{
	                edge.writeRecord(record);
	                //System.out.println(">> SENT RECORD ***");
	            }catch(Exception ex){
	            }
	        }
	        try {
				edge.eof();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    }
	} 
	
	private static class ConsumerThread extends Thread {
	    DataRecord record;
	    DirectEdge edge;
	    int counter;
	    
	    ConsumerThread(DataRecord record, DirectEdge edge){
	        this.record=record;
	        this.edge=edge;
	    }
	    
	    public void run(){
	        counter=0;
	        try{
	            while(edge.readRecord(record)!=null){
	                counter++;
	                //System.out.println("<< GOT RECORD ***");
	            }
	        }catch(Exception ex){
	        }
	    }
	    
	    public int getCounter(){
	        return counter;
	    }
	}
}
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
 * Created on 31.5.2005
 *
 */
package org.jetel.data.sequence;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.JetelException;

/**
 * @author david
 * @since  31.5.2005
 *
 * Simple class implementing Sequence interface. It uses internally "long" datatype to
 * store sequence's value. The value is persistent (stored on disk under specified name).
 * The class caches specified number of sequence values so it protects uniqueness of
 * generated values in various situations.
 */
public class SimpleSequence implements Sequence {
 
    public static final int DATA_SIZE = 8; //how many bytes occupy serialized value in file
    public static final String ACCESS_MODE="rwd";
    
    public static Log logger = LogFactory.getLog(SimpleSequence.class);
    
    String filename;
    String sequenceName;
    long sequenceValue;
    int step;
    int start;
    int numCachedValues;
    boolean isDefined=false;
  
    int counter;
    FileChannel io;
    FileLock lock;
    ByteBuffer buffer;
    
    /**
     * @param sequenceName name (should be unique) of the sequence to be created
     * @param filename filename (with full path) under which store sequence value's
     * @param start	the number the sequence should start with
     * @param step	the step to use when generating sequences
     * @param numCachedValues	how many values should be cached (reduces IO but consumes some of the 
     * available values)
     */
    public SimpleSequence(String sequenceName,String filename,int start,int step,int numCachedValues){
        this.sequenceName=sequenceName;
        this.filename=filename;
        this.start=start;
        this.sequenceValue=start;
        this.step=step;
        this.counter=0;
        this.numCachedValues=numCachedValues;
        this.isDefined=true;
    }
    
    public String getName(){
        return sequenceName;
    }
    
    public void setName(String sequenceName) {
    	this.sequenceName = sequenceName;
    }
    
    public long currentValueLong(){
        return sequenceValue;
        
    }
    
    public long nextValueLong(){
        if (counter<=0){
            flushValue(sequenceValue+step*numCachedValues);
            counter=numCachedValues;
        }
        sequenceValue+=step;
        counter--;
    
        return sequenceValue;
    }
    
    public int currentValueInt(){
        return (int)sequenceValue;
        
    }
    
    public int nextValueInt(){
        return (int)nextValueLong();
    }
    
    public String currentValueString(){
        return Long.toString(currentValueLong());
    }
    
    public String nextValueString(){
        return Long.toString(nextValueLong());
    }
    
    public void reset(){
        sequenceValue=start;
        flushValue(sequenceValue);
    }

    public boolean isPersistent(){
        return true;
    }
    
    public void init() throws JetelException{
        buffer=ByteBuffer.allocateDirect(DATA_SIZE);
        try{
            File file=new File(filename);
            if (!file.exists()){
                file.createNewFile();
                io=new RandomAccessFile(file,ACCESS_MODE).getChannel();
                lock=io.lock();
                io.force(true);
                flushValue(sequenceValue);
            }else{
                io=new RandomAccessFile(file,ACCESS_MODE).getChannel();
                lock=io.tryLock();
                if (lock==null){
                    // report non-locked sequence
                    logger.warn("Can't obtain file lock for sequence: "+getName());
                }
                io.force(true);
                io.read(buffer);
                buffer.flip();
                sequenceValue=buffer.getLong();
            }
        }catch(IOException ex){
            throw new JetelException(ex.getMessage());
        }
    }
    
    private final void flushValue(long value){
        try{
            buffer.rewind();
            buffer.putLong(value);
            buffer.flip();
            io.position(0);
            io.write(buffer);
        }catch(IOException ex){
            logger.error("I/O error when accessing sequence "+sequenceName+" - "+ex.getMessage());
            throw new RuntimeException("I/O error when accessing sequence "+sequenceName+" - "+ex.getMessage());
        }
    }
    
    public void close(){
        try{
            if (lock!=null){
                lock.release();
            }
            io.close();
        }catch(IOException ex){
            logger.error("I/O error when accessing sequence "+sequenceName+" - "+ex.getMessage());
            throw new RuntimeException("I/O error when accessing sequence "+sequenceName+" - "+ex.getMessage());
        }
    }
    
    public void delete(){
        File sequenceFile;
        close();
        sequenceFile=new File(filename);
        if (sequenceFile.exists()){;
            sequenceFile.delete();
        }
    }
    
	public int getNumCachedValues() {
		return numCachedValues;
	}

	public int getStart() {
		return start;
	}
	
	public int getStep() {
		return step;
	}
	
	public String getFilename() {
		return filename;
	}
	
}

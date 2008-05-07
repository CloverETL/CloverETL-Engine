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
package org.jetel.sequence;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * @author dpavlis
 * @since  31.5.2005
 * @revision    $Revision$
 *
 * Simple class implementing Sequence interface. It uses internally "long" datatype to
 * store sequence's value. The value is persistent (stored on disk under specified name).<br>
 * The class caches specified number of sequence values so it protects uniqueness of
 * generated values in various situations.<br>
 * <i>Note: by setting number of cached values to high enough value (>20) the performance
 * of SimpleSequence can be greatly increased.</i>
 *
 * The XML DTD describing the internal structure is as follows:
 * 
 * &lt;!ATTLIST Sequence
 *              id ID #REQUIRED
 *              type NMTOKEN (SIMPLE_SEQUENCE) #REQUIRED
 *              name CDATA #REQUIRED
 *              fileURL CDATA #REQUIRED
 *              start CDATA #IMPLIED
 *              step CDATA #IMPLIED
 *              cached CDATA #IMPLIED&gt;
 *                                 
 */
public class SimpleSequence extends GraphElement implements Sequence {

    public final static String SEQUENCE_TYPE = "SIMPLE_SEQUENCE";

    public static final int DATA_SIZE = 8; //how many bytes occupy serialized value in file
    public static final String ACCESS_MODE="rwd";
    
    public static Log logger = LogFactory.getLog(SimpleSequence.class);
    
    String filename;
    long sequenceValue;
    final int step;
    final int start;
    final int numCachedValues;
    boolean alreadyIncremented = false;
    
    int counter;
    FileChannel io;
    FileLock lock;
    ByteBuffer buffer;
    private static final String XML_NAME_ATTRIBUTE = "name";
	private static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
	private static final String XML_START_ATTRIBUTE = "start";
	private static final String XML_STEP_ATTRIBUTE = "step";
	private static final String XML_CACHED_ATTRIBUTE = "cached";
    
    /**
     * Creates SimpleSequence object.
     * 
     * @param id unique identifier of sequence
     * @param graph transformation graph, where is sequence placed
     * @param sequenceName name (should be unique) of the sequence to be created
     * @param filename filename (with full path) under which store sequence value's
     * @param start	the number the sequence should start with
     * @param step	the step to use when generating sequences
     * @param numCachedValues	how many values should be cached (reduces IO but consumes some of the 
     * available values between object reusals)
     */
    public SimpleSequence(String id, TransformationGraph graph, String sequenceName, String filename, int start, int step, int numCachedValues) {
        super(id, graph, sequenceName);
        this.filename=filename;
        this.start=start;
        this.sequenceValue=start;
        this.step=step;
        this.counter=0;
        this.numCachedValues=numCachedValues;
    }
    
    synchronized public long currentValueLong(){
        if(!isInitialized()) {
            throw new RuntimeException("Can't get currentValue for non-initialized sequence "+getId());
        }

        return alreadyIncremented ? sequenceValue - step : sequenceValue;
    }
    
    synchronized public long nextValueLong(){
        if(!isInitialized()) {
            throw new RuntimeException("Can't call nextValue for non-initialized sequence "+getId());
        }

        if (counter<=0){
            flushValue(sequenceValue+step*numCachedValues);
            counter=numCachedValues;
        }
        long tmpVal=sequenceValue;
        sequenceValue+=step;
        counter--;
        alreadyIncremented = true;
        
        return tmpVal;
    }
    
    public int currentValueInt(){
        return (int) currentValueLong();
    }
    
    public int nextValueInt(){
        return (int) nextValueLong();
    }
    
    public String currentValueString(){
        return Long.toString(currentValueLong());
    }
    
    public String nextValueString(){
        return Long.toString(nextValueLong());
    }
    
    /* (non-Javadoc)
     * @see org.jetel.data.sequence.Sequence#resetValue()
     */
    synchronized public void resetValue(){
        if(!isInitialized()) {
            throw new RuntimeException("Can't reset non-initialized sequence "+getId());
        }
        sequenceValue=start;
        alreadyIncremented = false;
        flushValue(sequenceValue);
    }

    public boolean isPersistent(){
        return true;
    }
    
    /**
     * Initializes sequence object. It is called after the sequence class is instantiated.
     * All necessary internal initialization should be performed in this method.
     */
    synchronized public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

        buffer = ByteBuffer.allocateDirect(DATA_SIZE);
        try{
            File file = new File(getGraph() != null ? 
            		FileUtils.getFile(getGraph().getProjectURL(), filename) :
            		filename);
            if (!file.exists()) {
            	logger.info("Sequence file " + filename + " doesn't exist. Creating new file.");
                file.createNewFile();
                io = new RandomAccessFile(file,ACCESS_MODE).getChannel();
                lock = io.lock();
                io.force(true);
                flushValue(sequenceValue);
            } else {
                io = new RandomAccessFile(file,ACCESS_MODE).getChannel();
                lock = io.tryLock();
                if (lock == null) {
                    // report non-locked sequence
                    logger.warn("Can't obtain file lock for sequence: " + getName()+" id: "+getId());
                }
                io.force(true);
                io.read(buffer);
                buffer.flip();
                sequenceValue = buffer.getLong();
                alreadyIncremented = false;
            }
        } catch(IOException ex) {
            free();
            throw new ComponentNotReadyException(this, XML_FILE_URL_ATTRIBUTE, ex.getMessage());
        }
    }

    @Override
    public synchronized void reset() throws ComponentNotReadyException {
    	super.reset();
    	
    	//DO NOTHING
    }
    
    private final void flushValue(long value) {
        try{
            buffer.rewind();
            buffer.putLong(value);
            buffer.flip();
            io.position(0);
            io.write(buffer);
        }catch(IOException ex){
            logger.error("I/O error when accessing sequence "+getName()+" id: "+getId()+" - "+ex.getMessage(),ex);
            throw new RuntimeException("I/O error when accessing sequence "+getName()+" id: "+getId()+" - "+ex.getMessage(),ex);
        }
    }
    
    /**
     * Closes the sequence (current instance). All internal resources should be freed in
     * this method.
     */
    synchronized public void free() {
        if(!isInitialized()) return;
        super.free();

        try {
            if (lock != null) {
                lock.release();
                lock=null;
            }
            if (io != null && io.isOpen()) {
                io.close();
                io=null;
            }
        } catch (IOException ex) {
            logger.error("I/O error when freeing sequence " + getName() + " - " + ex.getMessage(),ex);
            //throw new RuntimeException("I/O error when accessing sequence " + getName() + " - " + ex.getMessage());
        }
    }
    
    public synchronized void delete() {
        if(!isInitialized()) {
            throw new RuntimeException("Can't delete non-initialized sequence "+getId());
        }
        File sequenceFile;
        free();
        sequenceFile = new File(filename);
        if (sequenceFile.exists()) {
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

	public void setFilename(String filename) {
		if(isInitialized()) {
			throw new RuntimeException("Attempt to set file name for initialized sequence '" + toString() + "'.");
		}
		this.filename = filename;
	}

	static public SimpleSequence fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);

        try {
            return new SimpleSequence(
                xattribs.getString(XML_ID_ATTRIBUTE),
                graph,
				xattribs.getString(XML_NAME_ATTRIBUTE),
				xattribs.getString(XML_FILE_URL_ATTRIBUTE),
				xattribs.getInteger(XML_START_ATTRIBUTE),
                xattribs.getInteger(XML_STEP_ATTRIBUTE),
                xattribs.getInteger(XML_CACHED_ATTRIBUTE));
        } catch(Exception ex) {
            throw new XMLConfigurationException(SEQUENCE_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(), ex);
        }
	}

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        try {
			if (!FileUtils.canWrite(getGraph().getProjectURL(), filename)) {
	            throw new ComponentNotReadyException(this, XML_FILE_URL_ATTRIBUTE, "Can't write to " + filename);
			}
		} catch (ComponentNotReadyException e) {
			status.add(e, Severity.ERROR, this, Priority.NORMAL, e.getAttributeName());
		}
        return status;
    }

}

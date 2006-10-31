/*
 * Copyright (c) 2004-2005 OpenTech s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.graph;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jetel.data.DataRecord;
import org.jetel.data.tape.DataRecordTape;

/**
 * This class perform buffering data flowing through the edge.
 * If the edge has defined 'debugFile' attribute, this debugger save data into defined file.
 * 
 * @author Martin Zatopek
 *
 */
public class EdgeDebuger {

    private boolean isReadMode;
    
    private DataRecordTape dataTape;
    
    /**
     * Constructor.
     * @param debugFile
     */
    public EdgeDebuger(String debugFile, boolean isReadMode) {
        this.isReadMode = isReadMode;
        dataTape = new DataRecordTape(debugFile, !isReadMode, false);
    }
    
    public void init() throws IOException {
        dataTape.open();
        dataTape.addDataChunk();
        if(isReadMode) dataTape.rewind();
    }

    
    public void writeRecord(DataRecord record) throws IOException {
        if (isReadMode){
            throw new RuntimeException("Error: Mixed read/write operation on DataRecordTape !");
        }
        dataTape.put(record);
    }

    public void writeRecord(ByteBuffer record) throws IOException {
        if (isReadMode){
            throw new RuntimeException("Error: Mixed read/write operation on DataRecordTape !");
        }
        dataTape.put(record);
    }

    public DataRecord readRecord(DataRecord record) throws IOException {
        if (!isReadMode) {
            return null;
        }
        if (dataTape.get(record)){
            return record;
        }else{
            return null;
        }
    }

    public void open() {
        //EMPTY
    }

    public void close() {
        try {
            if(!isReadMode) {
                dataTape.flush(true);
            }
            dataTape.close();
        }catch(IOException ex){
            throw new RuntimeException("Can't flush/rewind DataRecordTape: " + ex.getMessage());
        }
    }
}

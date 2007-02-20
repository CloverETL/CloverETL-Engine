package org.jetel.util.ddl2clover;
public final class DataType {

    /** Data type, based on java.sql.Types */
    public final char type;

    /** Used when data type has fixed length */
    public final Number length;

    /** Used when data type has scale */
    public final Number scale;

    public DataType(char type, Number length, Number scale) {
        this.type = type;
        this.length = length;
        this.scale = scale;
    }

}
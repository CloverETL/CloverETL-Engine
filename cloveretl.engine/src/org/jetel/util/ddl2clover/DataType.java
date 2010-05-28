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
package org.jetel.util.ddl2clover;

public final class DataType {

    /** Data type, based on java.sql.Types */
    public final char type;

    /** Used when data type has fixed length */
    public final Number length;

    /** Used when data type has scale */
    public final Number scale;

    /** Used when data type has format */
    public String format;

    public DataType(char type, Number length, Number scale, String format) {
        this.type = type;
        this.length = length;
        this.scale = scale;
        this.format = format;
    }

}
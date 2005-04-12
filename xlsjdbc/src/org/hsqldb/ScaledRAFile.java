/* Copyright (c) 2001-2004, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;

import org.hsqldb.lib.HsqlByteArrayInputStream;

// fredt@users 20030111 - patch 1.7.2 by bohgammer@users - pad file before seek() beyond end

/**
 * This class is a wapper for a random access file such as that used for
 * CACHED table storage.
 *
 * The constructor takes a multiplier for positioning.
 * The seek(long position) method multiplies the position by the multiplier to
 * map to the underlying file.
 *
 * @author fredt@users
 * @version  1.7.2
 * @since  1.7.2
 */
class ScaledRAFile {

    static final int       DATA_FILE_RAF = 0;
    static final int       DATA_FILE_NIO = 1;
    final RandomAccessFile file;
    final int              scale;
    final private boolean  readOnly;
    final String           fileName;
    boolean                isNio;
    boolean                bufferDirty = true;

    static ScaledRAFile newScaledRAFile(String name, boolean readonly,
                                        int multiplier,
                                        int type)
                                        throws FileNotFoundException,
                                            IOException {

        if (type == DATA_FILE_RAF) {
            return new ScaledRAFile(name, readonly, multiplier);
        } else {
            try {
                Class.forName("java.nio.MappedByteBuffer");

                Class       c = Class.forName("org.hsqldb.NIOScaledRAFile");
                Constructor constructor = c.getConstructor(new Class[] {
                    String.class, boolean.class, int.class
                });

                return (ScaledRAFile) constructor.newInstance(new Object[] {
                    name, new Boolean(readonly), new Integer(multiplier)
                });
            } catch (Exception e) {
                return new ScaledRAFile(name, readonly, multiplier);
            }
        }
    }

    ScaledRAFile(String name, boolean readonly,
                 int multiplier) throws FileNotFoundException, IOException {

        file          = new RandomAccessFile(name, readonly ? "r"
                                                            : "rw");
        this.readOnly = readonly;
        scale         = multiplier;
        fileName      = name;
    }

    long length() throws IOException {
        return file.length();
    }

    /**
     * Some JVM's do not allow seek beyon end of file, so zeros are written
     * first in that case. Reported by bohgammer@users in Open Disucssion
     * Forum.
     */
    void seek(long position) throws IOException {

        if (file.length() < position) {
/*
            file.seek(file.length());

            for (long ix = file.length(); ix < position; ix++) {
                file.write(0);
            }
*/
        }

        file.seek(position);
    }

    long getFilePointer() throws IOException {
        return (file.getFilePointer() + scale - 1) / scale;
    }

    int read() throws IOException {
        return file.read();
    }

    void readOld(byte[] b, int offset, int length) throws IOException {
        file.readFully(b, offset, length);
    }

    int readIntOld() throws IOException {
        return file.readInt();
    }

    byte[]                   buffer = new byte[4096];
    HsqlByteArrayInputStream ba     = new HsqlByteArrayInputStream(buffer);
    long                     bufferBlockOffset;

    int readInt() throws IOException {

        long filePos        = file.getFilePointer();
        long maxBlockOffset = file.length() / buffer.length;
        long blockOffset    = file.getFilePointer() / buffer.length;
        long subOffset      = filePos % buffer.length;

        if (!bufferDirty && blockOffset == bufferBlockOffset) {
            file.seek(filePos + 4);
            ba.reset();
            ba.skip(subOffset);

            return ba.readInt();
        } else {
            if (blockOffset != maxBlockOffset) {
                bufferDirty = false;

                file.seek(filePos - subOffset);
                file.readFully(buffer, 0, buffer.length);
                file.seek(filePos + 4);

                bufferBlockOffset = blockOffset;

                ba.reset();
                ba.skip(subOffset);

                return ba.readInt();
            }
        }

        return file.readInt();
    }

    void read(byte[] b, int offset, int length) throws IOException {

        long filePos        = file.getFilePointer();
        long maxBlockOffset = file.length() / buffer.length;
        long blockOffset    = file.getFilePointer() / buffer.length;
        long subOffset      = filePos % buffer.length;

        if (!bufferDirty && blockOffset == bufferBlockOffset) {
            ba.reset();
            ba.skip(subOffset);

            int bytesRead = ba.read(b, offset, length);

            file.seek(filePos + bytesRead);

            if (bytesRead < length) {
                file.readFully(b, offset + bytesRead, length - bytesRead);
            }

            return;
        } else {
            if (blockOffset != maxBlockOffset
                    && subOffset + length <= buffer.length) {
                bufferDirty = false;

                file.readFully(buffer, 0, buffer.length);

                bufferBlockOffset = blockOffset;

                ba.reset();
                ba.skip(subOffset);
                ba.read(b, offset, length);
                file.seek(filePos + length);

                return;
            }
        }

        file.readFully(b, offset, length);
    }

    void write(byte[] b, int off, int len) throws IOException {

        bufferDirty = true;

        file.write(b, off, len);
    }

    void writeInt(int i) throws IOException {

        bufferDirty = true;

        file.writeInt(i);
    }

    void close() throws IOException {
        file.close();
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean wasNio() {
        return false;
    }
}

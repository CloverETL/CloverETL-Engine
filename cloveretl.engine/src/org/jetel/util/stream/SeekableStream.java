package org.jetel.util.stream;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;

public interface SeekableStream {
	
    /**
     * Returns this channel's position.
     *
     * @return  This channel's position,
     *          a non-negative integer counting the number of bytes
     *          from the beginning of the entity to the current position
     *
     * @throws  ClosedChannelException
     *          If this channel is closed
     * @throws  IOException
     *          If some other I/O error occurs
     */
    long position() throws IOException;

    /**
     * Sets this channel's position.
     *
     * <p> Setting the position to a value that is greater than the current size
     * is legal but does not change the size of the entity.  A later attempt to
     * read bytes at such a position will immediately return an end-of-file
     * indication.  A later attempt to write bytes at such a position will cause
     * the entity to grow to accommodate the new bytes; the values of any bytes
     * between the previous end-of-file and the newly-written bytes are
     * unspecified.
     *
     * <p> Setting the channel's position is not recommended when connected to
     * an entity, typically a file, that is opened with the {@link
     * java.nio.file.StandardOpenOption#APPEND APPEND} option. When opened for
     * append, the position is first advanced to the end before writing.
     *
     * @param  newPosition
     *         The new position, a non-negative integer counting
     *         the number of bytes from the beginning of the entity
     *
     * @return  This channel
     *
     * @throws  ClosedChannelException
     *          If this channel is closed
     * @throws  IllegalArgumentException
     *          If the new position is negative
     * @throws  IOException
     *          If some other I/O error occurs
     */
    SeekableStream position(long newPosition) throws IOException;

    /**
     * Returns the current size of entity to which this channel is connected.
     *
     * @return  The current size, measured in bytes
     *
     * @throws  ClosedChannelException
     *          If this channel is closed
     * @throws  IOException
     *          If some other I/O error occurs
     */
    long size() throws IOException;

    /**
     * Truncates the entity, to which this channel is connected, to the given
     * size.
     *
     * <p> If the given size is less than the current size then the entity is
     * truncated, discarding any bytes beyond the new end. If the given size is
     * greater than or equal to the current size then the entity is not modified.
     * In either case, if the current position is greater than the given size
     * then it is set to that size.
     *
     * <p> An implementation of this interface may prohibit truncation when
     * connected to an entity, typically a file, opened with the {@link
     * java.nio.file.StandardOpenOption#APPEND APPEND} option.
     *
     * @param  size
     *         The new size, a non-negative byte count
     *
     * @return  This channel
     *
     * @throws  NonWritableChannelException
     *          If this channel was not opened for writing
     * @throws  ClosedChannelException
     *          If this channel is closed
     * @throws  IllegalArgumentException
     *          If the new size is negative
     * @throws  IOException
     *          If some other I/O error occurs
     */
    SeekableStream truncate(long size) throws IOException;

}

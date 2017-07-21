package org.jetel.util.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.IllegalBlockingModeException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Adapter for {@link SeekableByteChannel} to an enhanced {@link OutputStream}.
 * 
 * Copied from {@link Channels#newOutputStream(WritableByteChannel)}.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2. 12. 2014
 */
public class SeekableOutputStream extends OutputStream implements SeekableStream {

	private final SeekableByteChannel channel;
	
    private ByteBuffer bb = null;
    private byte[] bs = null;       // Invoker's previous array
    private byte[] b1 = null;

    public SeekableOutputStream(SeekableByteChannel channel) {
		this.channel = channel;
	}

	@Override
	public long position() throws IOException {
		return channel.position();
	}

	@Override
	public SeekableOutputStream position(long newPosition) throws IOException {
		channel.position(newPosition);
		return this;
	}

	@Override
	public long size() throws IOException {
		return channel.size();
	}

	@Override
	public SeekableOutputStream truncate(long size) throws IOException {
		channel.truncate(size);
		return this;
	}

	@Override
	public synchronized void write(int b) throws IOException {
        if (b1 == null) {
            b1 = new byte[1];
        }
        b1[0] = (byte)b;
        this.write(b1);
	}

	/* (non-Javadoc)
	 * @see java.io.OutputStream#write(byte[], int, int)
	 */
	@Override
	public synchronized void write(byte[] bs, int off, int len) throws IOException {
        if ((off < 0) || (off > bs.length) || (len < 0) ||
                ((off + len) > bs.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        ByteBuffer bb = ((this.bs == bs)
                         ? this.bb
                         : ByteBuffer.wrap(bs));
        bb.limit(Math.min(off + len, bb.capacity()));
        bb.position(off);
        this.bb = bb;
        this.bs = bs;
        writeFully(channel, bb);
	}

	/* (non-Javadoc)
	 * @see java.io.OutputStream#close()
	 */
	@Override
	public void close() throws IOException {
		channel.close();
	}

	public SeekableByteChannel getChannel() {
		return channel;
	}

	/**
	 * Write all remaining bytes in buffer to the given channel.
	 * 
	 * @throws IllegalBlockingException
	 *             If the channel is selectable and configured non-blocking.
	 */
	private static void writeFully(WritableByteChannel ch, ByteBuffer bb) throws IOException {
		if (ch instanceof SelectableChannel) {
			SelectableChannel sc = (SelectableChannel) ch;
			synchronized (sc.blockingLock()) {
				if (!sc.isBlocking()) {
					throw new IllegalBlockingModeException();
				}
				writeFullyImpl(ch, bb);
			}
		} else {
			writeFullyImpl(ch, bb);
		}
	}

	/**
	 * Write all remaining bytes in buffer to the given channel. If the channel
	 * is selectable then it must be configured blocking.
	 */
	private static void writeFullyImpl(WritableByteChannel ch, ByteBuffer bb) throws IOException {
		while (bb.remaining() > 0) {
			int n = ch.write(bb);
			if (n <= 0) {
				throw new RuntimeException("no bytes written");
			}
		}
	}
	
}

/**
 * 
 */
package org.jetel.component;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

import org.jetel.component.DataRecordArrayPool.Stripe;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.ExternalSortDataRecord;
import org.jetel.data.ISortDataRecord;
import org.jetel.data.RecordComparator;
import org.jetel.data.formatter.BinaryDataFormatter;
import org.jetel.data.formatter.CloverDataFormatter;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.parser.BinaryDataParser;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.TaskQueueManager;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author pnajvar
 * @since Dec 2008
 * 
 */
public class ExtSort2 extends Node {

	private static final String XML_RUN_SIZE_ATTRIBUTE = "runSize";
	private static final String XML_READ_BUFFER_ATTRIBUTE = "readBuffer";
	private static final String XML_CONCURRENCY_LIMIT_ATTRIBUTE = "concurrencyLimit";
	private static final String XML_VERBOSE_ATTRIBUTE = "verbose";
	private static final String XML_SORTKEY_ATTRIBUTE = "sortKey";
	private static final String XML_TEMPORARY_DIRS = "tmpDirs";
	private static final String XML_TAPE_BUFFER_ATTRIBUTE = "tapeBuffer";
	private static final String XML_COMPRESS_ATTRIBUTE = "compress";

	public final static String COMPONENT_TYPE = "EXT_SORT2";

	
	/*
	 * Default run size
	 */
	public static final int DEFAULT_RUN_SIZE = 20000; // in records (20000)
	public static final int DEFAULT_TAPE_BUFFER = 32768; // in bytes (32768)
	public static final int DEFAULT_CONCURRENCY_LIMIT = 1; // in number of extra threads (1)
	public static final int DEFAULT_READ_BUFFER = 3; // number of read buffers must be greater or eq to concurrencyLimit
	
	private final static int READ_FROM_PORT = 0;
	private final static int WRITE_TO_PORT = 0;

	/*
	 * Ordering (true - asc, false - desc) for corresponding keys
	 * (sortKeysNames)
	 */
	private boolean[] sortOrderings;
	/*
	 * Fields that make up the sorting key
	 */
	private String[] sortKeysNames;
	/*
	 * Number of threads that perform read/write operations
	 */
	private int concurrencyLimit = DEFAULT_CONCURRENCY_LIMIT;
	/*
	 * Port to read from
	 */
	private InputPort inPort;
	/*
	 * Direct (byte) port to read from
	 */
	private InputPortDirect inPortDirect;
	/*
	 * temp records
	 */
	private DataRecord inRecord;
	private DataRecord tmpRecord;
	private DataRecord readRecord;
	/*
	 * length of single run (stripe, tape, .. whathever)
	 */
	private int runSize = DEFAULT_RUN_SIZE; // length in bytes for each tape
	/*
	 * unused
	 */
	private int readBuffer = DEFAULT_READ_BUFFER;
	/**
	 * A read buffer for each tape in bytes
	 * Warning - there must N * tapeBuffer (where N is number of tapes) available memory
	 * E.g. For 1000 tapes and tapeBuffer==32768(32KB) we need 32 MB free memory
	 */
	private int tapeBuffer = DEFAULT_TAPE_BUFFER;
	/*
	 * pool of temp directory names, if null, default system temp dir is used
	 */
	private File[] tmpDirs;
	/**
	 * index pointing to currently used tmp directory
	 */
	private int tmpDirCurrent; 
	/*
	 * A structure where created sorted runs are put for stage2
	 */
	HashMap<Integer, Parser> runsMap = new HashMap<Integer, Parser>();
	/*
	 * comparator used to sort DataRecords
	 */
	RecordComparator comparator;
	/*
	 * map used to perform merging (stage2)
	 */
	TreeMap<DataRecord, Tuple> sortingMap;

	
	boolean compressionEnabled;
	
	public boolean isCompressionEnabled() {
		return compressionEnabled;
	}

	public void setCompressionEnabled(boolean compressionEnabled) {
		this.compressionEnabled = compressionEnabled;
	}

	public int getTapeBuffer() {
		return tapeBuffer;
	}

	public void setTapeBuffer(int tapeBuffer) {
		this.tapeBuffer = tapeBuffer;
	}


	private final static boolean DEFAULT_ASCENDING_SORT_ORDER = true;
	private static final String KEY_FIELDS_ORDERING_1ST_DELIMETER = "(";
	private static final String KEY_FIELDS_ORDERING_2ND_DELIMETER = ")";

	public ExtSort2(String id, String[] sortKeys) {
		super(id);

		this.sortKeysNames = sortKeys;
		this.sortOrderings = new boolean[sortKeysNames.length];
		Arrays.fill(sortOrderings, true); // will be ascending in all fields by
		// default

		Pattern pat = Pattern.compile("^(.*)\\((.*)\\)$");

		for (int i = 0; i < sortKeys.length; i++) {
			Matcher matcher = pat.matcher(sortKeys[i]);
			if (matcher.find()) {
				String keyPart = sortKeys[i].substring(matcher.start(1),
						matcher.end(1));
				if (matcher.groupCount() > 1) {
					sortOrderings[i] = (sortKeys[i].substring(matcher.start(2),
							matcher.end(2))).matches("^[Aa].*");
				}
				sortKeys[i] = keyPart;
			}
		}


	}

	/**
	 * Constructs a RecordComparator based on particular metadata and settings
	 * 
	 * @param metaData
	 * @return
	 */
	private RecordComparator buildComparator(DataRecordMetadata metaData) {

		int[] fields = new int[this.sortKeysNames.length];

		for (int i = 0; i < fields.length; i++) {
			fields[i] = metaData.getFieldPosition(this.sortKeysNames[i]);
		}

		return new RecordComparator(fields);

	}

	/**
	 * This task performs 1st stage - stripping of input records into sorted
	 * runs (tapes) and writing these tapes to temp files
	 * 
	 * @author pnajvar
	 * 
	 */
	static class ReadAndSortTask extends Thread {

		ExtSort2 parent;
		InputPort inPort;
		Comparator<DataRecord> comparator;

		public ReadAndSortTask(ExtSort2 parent, InputPort port,
				Comparator<DataRecord> comparator) {
			this.parent = parent;
			this.inPort = port;
			this.comparator = comparator;
		}

		public void run() {
			System.out.println("ReadAndSortTask: Started task");

			int currentRunIndex = 0;
			Stripe stripe = null;
			long s1;
			long s0 = System.currentTimeMillis();
			long s2;
			try {
				
				Formatter out;

				do {

					if (parent.verbose) System.out.println("ReadAndSortTask[" + getName() + "] getting first stripe");
					s2 = System.currentTimeMillis();
					stripe = parent.inputPool.getNextFullStripe();
					parent.stage1WaitForFullBuffer += System.currentTimeMillis() - s2;
					if (parent.verbose) System.out.println("ReadAndSortTask[" + getName() + "] got full stripe " + stripe);

					if (stripe == null) {
						break;
					}

					currentRunIndex = parent.getNextRunIndex();

					// sort the run
					s1 = System.currentTimeMillis();

					Arrays.sort(stripe.getData(), 0, stripe.getSize(),
							comparator);
					parent.stage1sortingTime += System.currentTimeMillis() - s1;
					
					// put the run onto a new tape
					// TODO Use tmp dirs setting
					
					File tmp = File.createTempFile("run", ".dat", parent.pickTempDirectory());
					tmp.deleteOnExit();
					s1 = System.currentTimeMillis();

					// delimited text
					out = new DelimitedDataFormatter();
					// binary
//					out = new BinaryDataFormatter();
					// binary2
//					out = new DataFormatter("utf-8");
					out.init(inPort.getMetadata());
					out.close();
					OutputStream os = new FileOutputStream(tmp);
					if (parent.compressionEnabled) {
						os = new DeflaterOutputStream(os, new Deflater(Deflater.BEST_SPEED));
					}
					out.setDataTarget(os);
					for (int i = 0; i < stripe.getSize(); i++) {
						out.write(stripe.getData()[i]);
					}
					out.close();
					parent.stage1writeTime += System.currentTimeMillis() - s1;

					// now we can release the stripe for reuse
					// we won't need it any more
					stripe.clear(); // this is VERY important
					if (parent.verbose) System.out.println("ReadAndSortTask[" + getName() + "] releasing " + stripe);
					stripe.release();

					s1 = System.currentTimeMillis();
					// delimited text
					Parser in = new DelimitedDataParser();
					// binary
//					Parser in = new BinaryDataParser(parent.getTapeBuffer());
					// binary2
//					Parser in = new DataParser("utf-8");
					in.init(inPort.getMetadata());
					InputStream is = new FileInputStream(tmp);
					if (parent.compressionEnabled) {
						is = new InflaterInputStream(is);
					}
					in.setDataSource(is);
					
					parent.runsMap.put(currentRunIndex, in);
					parent.stage1initReaderTime += System.currentTimeMillis() - s1;

					// SynchronizeUtils.cloverYield();
				} while(true);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ComponentNotReadyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			parent.stage1sortThreadTime += System.currentTimeMillis() - s0;
			
		}

	}

	int currentRunIndex = 0;

	/**
	 * Picks tmp directories in round robin fashion
	 * Uses tmpDirs as a pool of directories 
	 * 
	 * Returns null if no tmpDirs is set
	 * 
	 * @return
	 */
	File pickTempDirectory() {
		if (this.tmpDirs != null) {
			return this.tmpDirs[(++tmpDirCurrent) % this.tmpDirs.length];
		} else {
			return null;
		}
	}
	
	public int getNextRunIndex() {
		return this.currentRunIndex++;
	}

	synchronized DataRecord readNextRecord() throws IOException,
			InterruptedException {
		tmpRecord = inPort.readRecord(inRecord);
		tmpRecord = tmpRecord != null ? inRecord.duplicate() : null;
		return tmpRecord;
	}

	DataRecordArrayPool inputPool;
	
	long stage1fillInputBufferTime;
	long stage1fillInputBufferPureTime;
	long stage1sortThreadTime;
	long stage1sortingTime;
	long stage1writeTime;
	long stage1initReaderTime;
	long stage1TimeTotal;
	long stage1WaitForFullBuffer;
	long stage1WaitForEmptyBuffer;
	

	long stage2outputWriteTime;
	long stage2parsingTime;
	long stage2outputReadTime;
	long stage2outputTimeTotal;

	/**
	 * This method fetches records from input port and puts them into stripes
	 * (unsorted runs) These full stripes then get picked up by ReadAndSortTask
	 * threads where they are sorted and put to disk
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 */
	void fillInputPool() throws IOException, InterruptedException {
		int currentStripeSize;
		int maxStripeSize = getRunSize();
		boolean terminated = false;
		inputPool.open();
		System.out.println("fillInputPool");
		long s1 = System.currentTimeMillis();
		long s2;
		while (runIt && !terminated) {

			if (verbose) System.out.print("fillInputPool(): getting next empty stripe..");
			s2 = System.currentTimeMillis();
			Stripe currentStripe = inputPool.getNextEmptyStripe();
			stage1WaitForEmptyBuffer += System.currentTimeMillis() - s2;
			if (verbose) System.out.println("got " + currentStripe + ", filling it");
			currentStripeSize = 0;
			s2 = System.currentTimeMillis();
			while (currentStripeSize < maxStripeSize) {
				tmpRecord = inPort.readRecord(inRecord);
				if (tmpRecord == null) {
					terminated = true;
					break;
				}
				currentStripe.getData()[currentStripeSize++].copyFrom(inRecord);
			}
			currentStripe.setSize(currentStripeSize);
			stage1fillInputBufferPureTime += System.currentTimeMillis() - s2;
			if (verbose) System.out.println("fillInputPool(): filled " + currentStripe + " with " + currentStripeSize + " records");
			currentStripe.release();
//			Thread.currentThread().yield();
		}
		inputPool.close();
		stage1fillInputBufferTime += System.currentTimeMillis() - s1;
	}

	boolean verbose = true;
	
	@Override
	public Result execute() throws Exception {

		inPort = getInputPort(READ_FROM_PORT);
		inPortDirect = getInputPortDirect(READ_FROM_PORT);
		inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();

		// first
		// we create sorted runs
		// a potentionally unlimited number of tapes containing sorted parts of
		// input

		comparator = buildComparator(inPort.getMetadata());

		// read records
		// create sorted runs (tapes) on disk
		long s1 = System.currentTimeMillis();

		DataRecord template = null;
		template = new DataRecord(inPort.getMetadata());
		template.init();

		System.out.println("Creating read buffer pool: " + getReadBuffer() + " x " + getRunSize() + " records, tape buffer is " + getTapeBuffer());
		inputPool = new DataRecordArrayPool(getReadBuffer(), getRunSize());
		inputPool.populate(template);
		inputPool.open(); // this is really important here.. the pool has to be open _prior_ any possible access by read task
		
		ReadAndSortTask[] taskPool = new ReadAndSortTask[getConcurrencyLimit()];

		for (int i = 0; i < taskPool.length; i++) {
			taskPool[i] = new ReadAndSortTask(this, inPort, comparator);
			taskPool[i].setName("ReadAndSort-" + i);
			taskPool[i].start();
		}

		// act as a fill thread for ReadAndSortTask threads
		fillInputPool();

		// merge sorted runs
		// we have to merge runs together
		// this is full processing
		// lets wait for all runs to get flushed to disk
		// taskManager.finish();
		for (ReadAndSortTask task : taskPool) {
			task.join();
		}

		// now merge
		stage1TimeTotal = System.currentTimeMillis() - s1;

		sortingMap = new TreeMap<DataRecord, Tuple>(comparator);

		// a page is a relatively small and finite number of records
		// records from each page are put into sortingMap which keeps it
		// sorted

		// initially, lets read first page from each tape
		for (Entry<Integer, Parser> entry : runsMap.entrySet()) {
			loadPage(entry.getKey(), entry.getValue());
		}

		// dump records
		Entry<DataRecord, Tuple> item;
		Tuple tuple;
		DataRecord r = new DataRecord(inPort.getMetadata());
		r.init();
		DataRecord r2;
		s1 = System.currentTimeMillis();
		long s2;
		long s3;
		while (!sortingMap.isEmpty()) {
			int[] toLoad = null;
			s2 = System.currentTimeMillis();
			
			Set<Entry<DataRecord, Tuple>> entrySet = sortingMap.entrySet();
			Iterator<Entry<DataRecord, Tuple>> iter = entrySet.iterator();
			
			while (iter.hasNext()) {
				item = iter.next();
				iter.remove();

				tuple = item.getValue();

				for (DataRecord record : tuple.records) {
					// outputQueue.put(record);
					writeRecord(WRITE_TO_PORT, record);
				}

				if (tuple.run != null) {
					toLoad = tuple.run;
					break;
				}
			}
			stage2outputWriteTime += System.currentTimeMillis() - s2;

			if (toLoad != null) {
				s2 = System.currentTimeMillis();
				for (int i = 0; i < toLoad.length; i++) {
//					loadPage(toLoad[i]);
					s3 = System.currentTimeMillis();
					r2 = runsMap.get(toLoad[i]).getNext(r);
					stage2parsingTime += System.currentTimeMillis() - s3;

					if (r2 != null) {
						r2 = r2.duplicate();
						Tuple t = new Tuple();
						Tuple old = sortingMap.put(r2, t);
						if (old != null) {
							t.run = old.run;
							t.records = old.records;
						}
						t.append(r2);
						t.appendRun(toLoad[i]);
					}
					
				}
				stage2outputReadTime += System.currentTimeMillis() - s2;
			}
		}
		stage2outputTimeTotal = System.currentTimeMillis() - s1;

		// stats
//		for(Entry<Integer, Parser> en : runsMap.entrySet()) {
//			if (en.getValue() instanceof BinaryDataParser) {
//				((BinaryDataParser) en.getValue()).printStats();
//			}
//		}
		
		System.out.println("ExtSort2:Performance summary");
		System.out.println("Stage 1 - read sort and write runs (total): " + (stage1TimeTotal / 1000.0));
		System.out.println("- reading (total): " + (stage1fillInputBufferTime/ 1000.0));
		System.out.println("-     pure: " + (stage1fillInputBufferPureTime / 1000.0));
		System.out.println("- sorting (paralell and cumulative) : " + (stage1sortingTime / 1000.0));
		System.out.println("- writing runs (cumulative): " + (stage1writeTime / 1000.0));
		System.out.println("- 	init reader(cumulative): " + (stage1initReaderTime / 1000.0));
		System.out.println("- sort threads: all - " + (stage1sortThreadTime / 1000.0) + " avg: " + ((stage1sortThreadTime / 1000.0 / (double) getConcurrencyLimit())));
		System.out.println("- wait for empty stripe (filler blocked): " + (stage1WaitForEmptyBuffer / 1000.0));
		System.out.println("- wait for full buffer stripe (read&sort blocked): " + (stage1WaitForFullBuffer / 1000.0));

		System.out.println("Stage 2 - merge runs and output: " + (stage2outputTimeTotal / 1000.0));
		System.out.println("-  read time: "	+ (stage2outputReadTime / 1000.0));
		System.out.println("-    parsing: " + (stage2parsingTime / 1000.0));
		System.out.println("- write: " + (stage2outputWriteTime / 1000.0));

		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	void loadPage(int runIndex) throws JetelException {
		loadPage(runIndex, runsMap.get(runIndex));
	}

	HashMap<Long, Integer> recCounts = new HashMap<Long, Integer>();

	void loadPage(int runIndex, Parser source)
			throws JetelException {
		DataRecord r;
		r = source.getNext();
		if (r == null) {
			// System.err.println("Finished reading records from run " +
			// runIndex + " (total " + recCounts.get(runIndex) + ")");
			return; // no more records
		}
		// Integer n = recCounts.get(runIndex);
		// if (n == null) {
		// recCounts.put(runIndex, 1);
		// } else {
		// recCounts.put(runIndex, n.intValue()+1);
		// }

		Tuple t = new Tuple();
		Tuple old = sortingMap.put(r, t);
		if (old != null) {
			t.run = old.run;
			t.records = old.records;
		}

		t.append(r);

		t.appendRun(runIndex);
	}

	protected static class Tuple {

		public ArrayList<DataRecord> records = new ArrayList<DataRecord>();
		public int[] run;

		public Tuple() {
		}

		public final void appendRun(int runNumber) {

			if (run == null) {
				run = new int[] { runNumber };
				return;
			}

			int size = run.length + 1;
			int[] tmp = new int[size];
			System.arraycopy(run, 0, tmp, 0, run.length);
			tmp[size - 1] = runNumber;
			run = tmp;
		}

		public void append(DataRecord record) {
			records.add(record);
		}

	}

	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException
	 *                Description of the Exception
	 * @since April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();

		// try {
		// // create sorter
		// sorter = new ExternalSortDataRecord(getInputPort(READ_FROM_PORT)
		// .getMetadata(), sortKeysNames, sortOrderings, internalBufferCapacity,
		// DEFAULT_NUMBER_OF_TAPES, tmpDirs);
		// } catch (Exception e) {
		// throw new ComponentNotReadyException(e);
		// }
		//
		if (getConcurrencyLimit() > 0) {
			taskManager = new TaskQueueManager(getConcurrencyLimit(), 1);
		}

	}

	TaskQueueManager taskManager;

	@Override
	public void free() {
		if (!isInitialized())
			return;
		super.free();
		// if (sorter != null) {
		// try {
		// sorter.free();
		// } catch (InterruptedException e) {
		// //DO NOTHING
		// }
		// }
	}

	public int getConcurrencyLimit() {
		return concurrencyLimit;
	}

	public void setConcurrencyLimit(int concurrentLimit) {
		this.concurrencyLimit = concurrentLimit;
	}

	public void reset() throws ComponentNotReadyException {
		super.reset();
		// sorter.reset();
	}

	/**
	 * Description of the Method
	 * 
	 * @return Description of the Returned Value
	 * @since May 21, 2002
	 */
	public void toXML(org.w3c.dom.Element xmlElement) {
		super.toXML(xmlElement);

		// sortKey attribute
		String sortKeys = this.sortKeysNames[0]
				+ KEY_FIELDS_ORDERING_1ST_DELIMETER + this.sortOrderings[0]
				+ KEY_FIELDS_ORDERING_2ND_DELIMETER;
		for (int i = 1; i < this.sortKeysNames.length; i++) {
			sortKeys += Defaults.Component.KEY_FIELDS_DELIMITER
					+ sortKeysNames[i] + KEY_FIELDS_ORDERING_1ST_DELIMETER
					+ this.sortOrderings[i] + KEY_FIELDS_ORDERING_2ND_DELIMETER;
		}

		xmlElement.setAttribute(XML_SORTKEY_ATTRIBUTE, sortKeys);

		// sortOrder attribute - deprecated
		/*
		 * xmlElement.setAttribute(XML_SORTORDER_ATTRIBUTE,
		 * sortOrderingsString);
		 */

		if (getRunSize() != DEFAULT_RUN_SIZE) {
			xmlElement.setAttribute(XML_RUN_SIZE_ATTRIBUTE, String
					.valueOf(getRunSize()));
		}

		if (getReadBuffer() != DEFAULT_READ_BUFFER) {
			xmlElement.setAttribute(XML_READ_BUFFER_ATTRIBUTE, String
					.valueOf(getReadBuffer()));
		}

		if (getConcurrencyLimit() != DEFAULT_CONCURRENCY_LIMIT) {
			xmlElement.setAttribute(XML_CONCURRENCY_LIMIT_ATTRIBUTE, String
					.valueOf(getConcurrencyLimit()));
		}

		if (tmpDirs != null && tmpDirs.length > 0) {
			StringBuffer sb = new StringBuffer();
			for(int i = 0; i < tmpDirs.length; i++) {
				if (i > 0) {
					sb.append(";");
				}
				if (tmpDirs[i] != null) {
					sb.append(tmpDirs[i].getPath());
				}
			}
			
			xmlElement.setAttribute(XML_TEMPORARY_DIRS, sb.toString());
		}
	}

	/**
	 * Description of the Method
	 * 
	 * @param nodeXML
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @since May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement)
			throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(
				xmlElement, graph);

		ExtSort2 sort;

		try {

			sort = new ExtSort2(xattribs.getString(XML_ID_ATTRIBUTE), xattribs
					.getString(XML_SORTKEY_ATTRIBUTE).split(
							Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));

			if (xattribs.exists(XML_READ_BUFFER_ATTRIBUTE)) {
				sort.setReadBuffer(xattribs
						.getInteger(XML_READ_BUFFER_ATTRIBUTE));
			}

			if (xattribs.exists(XML_RUN_SIZE_ATTRIBUTE)) {
				sort.setRunSize(xattribs.getInteger(XML_RUN_SIZE_ATTRIBUTE));
			}

			if (xattribs.exists(XML_CONCURRENCY_LIMIT_ATTRIBUTE)) {
				sort.setConcurrencyLimit(xattribs
						.getInteger(XML_CONCURRENCY_LIMIT_ATTRIBUTE));
			}

			if (xattribs.exists(XML_TEMPORARY_DIRS)) {
				String[] dirs = xattribs.getString(XML_TEMPORARY_DIRS).split(
						Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
				ArrayList<File> tmpDirsTmp = new ArrayList<File>();
				File f = null;
				for(String dir : dirs) {
					if (dir != null && dir.trim().length() > 0) {
						f = new File(dir);
						if (f.exists() && f.isDirectory() && f.canWrite()) {
							tmpDirsTmp.add(f);
						}
					}
				}
				sort.setTmpDirs( tmpDirsTmp.size() > 0 ? tmpDirsTmp.toArray(new File[tmpDirsTmp.size()]) : null);
			}

			if (xattribs.exists(XML_VERBOSE_ATTRIBUTE)) {
				sort.verbose = xattribs.getBoolean(XML_VERBOSE_ATTRIBUTE, false);
			}

			if (xattribs.exists(XML_TAPE_BUFFER_ATTRIBUTE)) {
				sort.setTapeBuffer(xattribs.getInteger(XML_TAPE_BUFFER_ATTRIBUTE, DEFAULT_TAPE_BUFFER));
			}

			if (xattribs.exists(XML_COMPRESS_ATTRIBUTE)) {
				sort.setCompressionEnabled(xattribs.getBoolean(XML_COMPRESS_ATTRIBUTE, false));
			}
		
			
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":"
					+ xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ")
					+ ":" + ex.getMessage(), ex);
		}
		return sort;
	}

	public int getReadBuffer() {
		return readBuffer < getConcurrencyLimit() ? getConcurrencyLimit()
				: readBuffer;
	}

	public void setReadBuffer(int readBuffer) {
		this.readBuffer = readBuffer;
	}

	private int getSortKeyCount() {
		return sortKeysNames.length;
	}

	public int getRunSize() {
		return runSize;
	}

	public void setRunSize(int runSize) {
		this.runSize = runSize;
	}

	private void setSortOrders(String string, int minLength) {

		String[] tmp = string
				.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		sortOrderings = new boolean[Math.max(tmp.length, minLength)];
		boolean lastValue = true;

		for (int i = 0; i < tmp.length; i++) {
			lastValue = sortOrderings[i] = tmp[i].matches("^[Aa].*");
		}

		for (int i = tmp.length; i < minLength; i++) {
			sortOrderings[i] = lastValue;
		}
	}

	/**
	 * Description of the Method
	 * 
	 * @return Description of the Return Value
	 */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 1, 1)
				|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}

		checkMetadata(status, getInMetadata(), getOutMetadata());

		try {
			init();
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(e
					.getMessage(), ConfigurationStatus.Severity.ERROR, this,
					ConfigurationStatus.Priority.NORMAL);
			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
		} finally {
			free();
		}

		return status;
	}

	public File[] getTmpDirs() {
		return tmpDirs;
	}

	public void setTmpDirs(File[] tmpDirs) {
		this.tmpDirs = tmpDirs;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.Node#getType()
	 */
	public String getType() {
		return COMPONENT_TYPE;
	}

}

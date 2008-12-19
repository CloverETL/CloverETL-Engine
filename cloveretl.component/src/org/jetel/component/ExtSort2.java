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

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.ExternalSortDataRecord;
import org.jetel.data.ISortDataRecord;
import org.jetel.data.RecordComparator;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.parser.DelimitedDataParser;
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
	private static final String XML_SORTKEY_ATTRIBUTE = "sortKey";
	private static final String XML_TEMPORARY_DIRS = "tmpDirs";

	public final static String COMPONENT_TYPE = "EXT_SORT2";

	private final static int READ_FROM_PORT = 0;
	private final static int WRITE_TO_PORT = 0;

	private boolean[] sortOrderings;
	private String[] sortKeysNames;

	private int concurrencyLimit = 1;

	private InputPort inPort;
	private InputPortDirect inPortDirect;
	private DataRecord inRecord;
	private DataRecord tmpRecord;

	public int getRunSize() {
		return runSize;
	}

	public void setRunSize(int runSize) {
		this.runSize = runSize;
	}

	private int runSize; // length in bytes for each tape
	private int readBuffer; // number of records stored in read thread in 1st
							// stage
	private int outputQueueLength;
	private String[] tmpDirs;
	private boolean useReadQueue = true;

	/*
	 * Default run size of 20 MB
	 */
	public static final int DEFAULT_RUN_SIZE = 50000;

	/*
	 * Default page size must be able to store at least one record
	 */
	public static final int DEFAULT_PAGE_SIZE = 1;

	public static final int DEFAULT_OUTPUT_QUEUE_LENGTH = 1000000;

	private final static boolean DEFAULT_ASCENDING_SORT_ORDER = true;
	private static final String KEY_FIELDS_ORDERING_1ST_DELIMETER = "(";
	private static final String KEY_FIELDS_ORDERING_2ND_DELIMETER = ")";

	long startTime;
	long startTime1;
	long startTime2;
	long startTime3;
	long readingTime;
	long readingFetchTime;
	long readingFetchPureTime;
	long readingStoringTime;
	long readingTimeTotal;
	long sortingTime;
	long outputTime;
	long outputWriteTime;
	long outputReadTime;
	long parsingTime;

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

		this.runSize = DEFAULT_RUN_SIZE;
		this.useReadQueue = true;
		this.readBuffer = -1;
		this.outputQueueLength = DEFAULT_OUTPUT_QUEUE_LENGTH;

		this.outputQueue = new ArrayBlockingQueue<DataRecord>(outputQueueLength);

	}

	private RecordComparator buildComparator(DataRecordMetadata metaData) {

		int[] fields = new int[this.sortKeysNames.length];

		for (int i = 0; i < fields.length; i++) {
			fields[i] = metaData.getFieldPosition(this.sortKeysNames[i]);
		}

		return new RecordComparator(fields);

	}

	HashMap<Long, DelimitedDataParser> runsMap = new HashMap<Long, DelimitedDataParser>();

	RecordComparator comparator;
	TreeMap<DataRecord, Tuple> sortingMap;
	DataRecord readRecord;

	ArrayBlockingQueue<DataRecord> outputQueue;

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
		boolean readFromQueue;
		DataRecord killerRecord;

		public ReadAndSortTask(ExtSort2 parent, InputPort port,
				Comparator<DataRecord> comparator, boolean readFromQueue) {
			this.parent = parent;
			this.inPort = port;
			this.comparator = comparator;
			this.readFromQueue = readFromQueue;
		}

		public void run() {
			long currentRunIndex = 0;
			int currentRunSize;
			int maxRunSize = parent.getRunSize();
			// DataRecord[] memoryRun = new DataRecord[maxRunSize];

			DataRecord inRecord = new DataRecord(inPort.getMetadata());
			inRecord.init();
			DataRecord tmpRecord = inRecord;

			System.out.println("ReadAndSortTask: Started task");

			TreeMap<DataRecord, ArrayList<DataRecord>> run = new TreeMap<DataRecord, ArrayList<DataRecord>>(
					comparator);
			ArrayList<DataRecord> list;

			long startTime1;
			long startTime2;
			long startTime3;

			try {
				while (parent.runIt) {
					currentRunIndex = parent.getNextRunIndex();
					currentRunSize = 0;
					// memoryRun.clear();
					run.clear();
					startTime1 = System.currentTimeMillis();
//					System.out.println("Thread [" + getName() + "] started reading run " + currentRunIndex);

					while (currentRunSize < maxRunSize) {
						startTime2 = System.currentTimeMillis();
						if (readFromQueue) {
							startTime3 = System.currentTimeMillis();
							tmpRecord = parent.recordReadQueue.take();
							parent.readingFetchPureTime += System
									.currentTimeMillis()
									- startTime3;
						} else {
							tmpRecord = parent.readNextRecord();
						}
						parent.readingFetchTime += System.currentTimeMillis()
								- startTime2;
						if (tmpRecord == killerRecord) {
							return;
						}

						startTime2 = System.currentTimeMillis();
						list = run.get(tmpRecord);
						if (list == null) {
							list = new ArrayList<DataRecord>();
							run.put(tmpRecord, list);
						}
						list.add(tmpRecord);
						parent.readingStoringTime += System.currentTimeMillis()
								- startTime2;
						// memoryRun[currentRunSize] = tmpRecord;
						currentRunSize++;
//						Thread.yield();
					}
//					System.out.println("Thread [" + getName() + "] finished reading run " + currentRunIndex);
					parent.readingTime += System.currentTimeMillis()
							- startTime1;
					if (currentRunSize == 0) {
						break;
					}

					// sort the run
					// Collections.sort(run, comparator);
					// Arrays.sort(memoryRun, 0, currentRunSize, comparator);

					// put the run onto a new tape
					// TODO Use tmp dirs setting
//					System.out.println("Thread [" + getName() + "] started sorting run " + currentRunIndex);

					File tmp = File.createTempFile("run", ".dat");
					tmp.deleteOnExit();

					DelimitedDataFormatter out = new DelimitedDataFormatter();
					out.setDataTarget(new BufferedOutputStream(
							new FileOutputStream(tmp)));
					out.init(inPort.getMetadata());

					// for (int i = 0; i < currentRunSize; i++) {
					// out.write(memoryRun[i]);
					// }
					startTime2 = System.currentTimeMillis();
					Set<Entry<DataRecord, ArrayList<DataRecord>>> entrySet = run
							.entrySet();
					Iterator<Entry<DataRecord, ArrayList<DataRecord>>> iter = entrySet
							.iterator();
					Entry<DataRecord, ArrayList<DataRecord>> item;
					while (iter.hasNext()) {
						item = iter.next();
//						Thread.yield();
						for (DataRecord r : item.getValue()) {
							out.write(r);
						}
					}

					out.close();

					DelimitedDataParser ddp = new DelimitedDataParser();
					ddp.init(inPort.getMetadata());
					ddp.setDataSource(new BufferedInputStream(
							new FileInputStream(tmp)));
					parent.sortingTime += System.currentTimeMillis()
							- startTime2;
					parent.runsMap.put(currentRunIndex, ddp);
//					System.out.println("Thread [" + getName() + "] finished sorting run " + currentRunIndex);

					// SynchronizeUtils.cloverYield();
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ComponentNotReadyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		void setKillerRecord(DataRecord r) {
			this.killerRecord = r;
		}

	}

	long currentRunIndex = 0;

	public long getNextRunIndex() {
		return this.currentRunIndex++;
	}

	synchronized DataRecord readNextRecord() throws IOException,
			InterruptedException {
		startTime3 = System.currentTimeMillis();

		tmpRecord = inPort.readRecord(inRecord);
		tmpRecord = tmpRecord != null ? inRecord.duplicate() : null;
		readingFetchPureTime += System.currentTimeMillis() - startTime3;
		return tmpRecord;
	}

	BlockingQueue<DataRecord> recordReadQueue;

	
	
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
		startTime = System.currentTimeMillis();

		DataRecord killerRecord = null;
		if (this.useReadQueue) {
			killerRecord = new DataRecord(inPort.getMetadata());
			recordReadQueue = new LinkedBlockingQueue<DataRecord>(getReadBuffer());
		}

		ReadAndSortTask[] taskPool = new ReadAndSortTask[getConcurrencyLimit()];

		for (int i = 0; i < taskPool.length; i++) {
			taskPool[i] = new ReadAndSortTask(this, inPort, comparator,
					this.useReadQueue);
			taskPool[i].setKillerRecord(killerRecord);
//			taskManager.addTask(taskPool[i]);
			taskPool[i].setName("ReadAndSort-" + i);
			taskPool[i].start();
		}

		// merge sorted runs
		if (this.useReadQueue) {

			while (runIt) {
				tmpRecord = inPort.readRecord(inRecord);
				if (tmpRecord == null) {
					break;
				} else {
					recordReadQueue.put(inRecord.duplicate());
				}
			}

			// lets kill all tasks

			for (int i = 0; i < getConcurrencyLimit(); i++) {
				recordReadQueue.put(killerRecord);
			}

		}
		// we have to merge runs together
		// this is full processing
		// lets wait for all runs to get flushed to disk
//		taskManager.finish();
		for(ReadAndSortTask task : taskPool) {
			task.join();
		}
		// now merge

		readingTimeTotal = System.currentTimeMillis() - startTime;

		sortingMap = new TreeMap<DataRecord, Tuple>(comparator);

		// a page is a relatively small and finite number of records
		// records from each page are put into sortingMap which keeps it
		// sorted

		// initially, lets read first page from each tape
		for (Entry<Long, DelimitedDataParser> entry : runsMap.entrySet()) {
			loadPage(entry.getKey(), entry.getValue());
		}

		// dump records
		Entry<DataRecord, Tuple> item;
		Tuple tuple;
		DataRecord r = new DataRecord(inPort.getMetadata());
		r.init();
		DataRecord r2;
		startTime = System.currentTimeMillis();
		while (!sortingMap.isEmpty()) {
			long[] toLoad = null;
			startTime1 = System.currentTimeMillis();
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
			outputWriteTime += System.currentTimeMillis() - startTime1;

			if (toLoad != null) {
				startTime1 = System.currentTimeMillis();
				for (int i = 0; i < toLoad.length; i++) {
					// loadPage(toLoad[i]);
					startTime2 = System.currentTimeMillis();
					r2 = runsMap.get(toLoad[i]).getNext(r);
					parsingTime += System.currentTimeMillis() - startTime2;
					if (r2 != null) {
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
				outputReadTime += System.currentTimeMillis() - startTime1;
			}
		}
		outputTime = System.currentTimeMillis() - startTime;

		System.out.println("ExtSort2:Performance summary");
		System.out.println("- reading time: " + (readingTime / 1000.0));
		System.out.println("-     fetching: " + (readingFetchTime / 1000.0));
		System.out.println("-     fetching pure: "
				+ (readingFetchPureTime / 1000.0));
		System.out.println("-     storing : " + (readingStoringTime / 1000.0));

		System.out.println("- sorting time: " + (sortingTime / 1000.0));
		System.out.println("- reading phase total: "
				+ (readingTimeTotal / 1000.0));

		System.out.println("- output write: " + (outputWriteTime / 1000.0));
		System.out.println("- output read: " + (outputReadTime / 1000.0));
		System.out.println("-    parsing: " + (parsingTime / 1000.0));

		System.out.println("- output phase total: " + (outputTime / 1000.0));

		// DataRecord lastRecord = new DataRecord(inPort.getMetadata());
		// dumper.lastRecord = lastRecord;
		// outputQueue.put(lastRecord); // this wakes up the dumper and
		// makes it quit

		// try {
		// sorter.sort();
		// } catch (InterruptedException ex) {
		// throw ex;
		// } catch (Exception ex) {
		// throw new JetelException( "Error when sorting: " +
		// ex.getMessage(),ex);
		// }

		// while (sorter.get(recordBuffer) && runIt) {
		// writeRecordBroadcastDirect(recordBuffer);
		// recordBuffer.clear();
		// }

		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	protected class DumperThread extends Thread {
		DataRecord lastRecord;

		public void run() {
			DataRecord record;
			try {
				while (true) {

					record = outputQueue.take();
					if (record == lastRecord) {
						return;
					}
					writeRecordBroadcast(record);

				}
			} catch (Exception e) {
			}
		}
	}

	void sortRun(DataRecord[] run, long runIndex) throws Exception {
//		startTime2 = System.currentTimeMillis();
		// sort the run
		// Collections.sort(run, comparator);
		Arrays.sort(run, comparator);

		// put the run onto a new tape
		// TODO Use tmp dirs setting
		File tmp = File.createTempFile("run", ".dat");
		tmp.deleteOnExit();

		DelimitedDataFormatter out = new DelimitedDataFormatter();
		out.setDataTarget(new FileOutputStream(tmp));
		out.init(inPort.getMetadata());

		for (DataRecord r : run) {
			out.write(r);
		}
		out.close();

		DelimitedDataParser ddp = new DelimitedDataParser();
		ddp.init(inPort.getMetadata());
		ddp.setDataSource(new FileInputStream(tmp));

		runsMap.put(runIndex, ddp);
//		sortingTime += System.currentTimeMillis();
	}

	void loadPage(long runIndex) throws JetelException {
		loadPage(runIndex, runsMap.get(runIndex));
	}

	HashMap<Long, Integer> recCounts = new HashMap<Long, Integer>();

	void loadPage(long runIndex, DelimitedDataParser source)
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
		public long[] run;

		public Tuple() {
		}

		public final void appendRun(long runNumber) {

			if (run == null) {
				run = new long[] { runNumber };
				return;
			}

			int size = run.length + 1;
			long[] tmp = new long[size];
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

		if (getReadBuffer() != -1) {
			xmlElement.setAttribute(XML_READ_BUFFER_ATTRIBUTE, String
					.valueOf(getReadBuffer()));
		}
		
		if (getConcurrencyLimit() != 1) {
			xmlElement.setAttribute(XML_CONCURRENCY_LIMIT_ATTRIBUTE, String
					.valueOf(getConcurrencyLimit()));
		}

		if (tmpDirs != null) {
			xmlElement.setAttribute(XML_TEMPORARY_DIRS, StringUtils
					.stringArraytoString(tmpDirs, ';'));
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
				sort.setReadBuffer(xattribs.getInteger(XML_READ_BUFFER_ATTRIBUTE));
			}

			if (xattribs.exists(XML_RUN_SIZE_ATTRIBUTE)) {
				sort.setRunSize(xattribs.getInteger(XML_RUN_SIZE_ATTRIBUTE));
			}

			if (xattribs.exists(XML_CONCURRENCY_LIMIT_ATTRIBUTE)) {
				sort.setConcurrencyLimit(xattribs
						.getInteger(XML_CONCURRENCY_LIMIT_ATTRIBUTE));
			}

			if (xattribs.exists(XML_TEMPORARY_DIRS)) {
				sort.setTmpDirs(xattribs.getString(XML_TEMPORARY_DIRS).split(
						Defaults.DEFAULT_PATH_SEPARATOR_REGEX));
			}

		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":"
					+ xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ")
					+ ":" + ex.getMessage(), ex);
		}
		return sort;
	}

	public int getReadBuffer() {
		return readBuffer < 0 ? getConcurrencyLimit() * getRunSize() * 2 : readBuffer;
	}

	public void setReadBuffer(int readBuffer) {
		this.readBuffer = readBuffer;
		this.useReadQueue = readBuffer != 0;
	}

	private int getSortKeyCount() {
		return sortKeysNames.length;
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

	public String[] getTmpDirs() {
		return tmpDirs;
	}

	public void setTmpDirs(String[] tmpDirs) {
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

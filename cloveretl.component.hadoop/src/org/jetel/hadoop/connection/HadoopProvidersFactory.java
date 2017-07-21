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
package org.jetel.hadoop.connection;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.hadoop.component.IHadoopSequenceFileFormatter;
import org.jetel.hadoop.component.IHadoopSequenceFileParser;
import org.jetel.hadoop.service.AbstractHadoopConnectionData;
import org.jetel.hadoop.service.HadoopConnectingService;
import org.jetel.hadoop.service.filesystem.HadoopConnectingFileSystemService;
import org.jetel.hadoop.service.filesystem.HadoopDataInput;
import org.jetel.hadoop.service.filesystem.HadoopDataOutput;
import org.jetel.hadoop.service.filesystem.HadoopFileStatus;
import org.jetel.hadoop.service.filesystem.HadoopFileSystemConnectionData;
import org.jetel.hadoop.service.mapreduce.HadoopConnectingMapReduceService;
import org.jetel.hadoop.service.mapreduce.HadoopCounterGroup;
import org.jetel.hadoop.service.mapreduce.HadoopJobReporter;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceConnectionData;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceInfoService;
import org.jetel.hadoop.service.mapreduce.HadoopMapReduceJob;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.classloader.GreedyURLClassLoader;

/**
 * <p> Static factory class for instantiating providers for Hadoop services of required Hadoop API version. It also
 * supports other version related operations like providing list of supported Hadoop API versions. Together with service
 * layer this class allows abstraction from different Hadoop API versions. </p>
 * 
 * <p> This class also creates a proxy (a wrapper class that delegates all the methods to the wrapped, original
 * provider) for each service before returning it. This proxy classes ensure that providers methods are called in
 * correct context - that is with the right context class loader set (see
 * {@link Thread#setContextClassLoader(ClassLoader)}). Proxy classes are private and therefore invisible to client nor
 * does the provider implementation recognize that its method are beeing called through proxy. </p>
 * 
 * <p>This class contains static factory methods for creating providers of these services for required Hadoop API
 * versions: <ul> <li>{@link HadoopConnectingFileSystemService}</li> <li>{@link HadoopConnectingMapReduceService}</li>
 * <li>{@link HadoopMapReduceInfoService}</li> </ul> </p>
 * 
 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin, a.s
 *         (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
 * @since rel-3-4-0-M2
 * @created 16.11.2012
 * @see HadoopConnection
 */
public final class HadoopProvidersFactory {

	public static final String HADOOP_VERSION = "hadoop-0.20.2";
	public static final String FS_PROVIDER_KEY = "fsProviderKey";
	public static final String MAPRED_PROVIDER_KEY = "mapredProviderKey";
	public static final String MAPRED_INFO_PROVIDER_KEY = "mapredInfoProviderKey";
	
	private static Map<Set<URL>, ClassLoader> classLoaderCache = new ConcurrentHashMap<Set<URL>, ClassLoader>();
	
	private static final Log LOG = LogFactory.getLog(HadoopProvidersFactory.class);
	
	/**
	 * Gets list of supported Hadoop API versions.
	 * @return List of supported Hadoop versions for which providers are available.
	 */
	public static List<String> getSupportedHadoopVersions() {
		return Arrays.asList(new String[] { HADOOP_VERSION });
	}

	/**
	 * Indicates whether given Hadoop version is supported.
	 * @param version Hadoop version to be checked.
	 * @return <code>true</code> if and only if specified Hadoop API version is supported that is if service providers
	 *         can be obtained for that version using static methods of this class.
	 */
	public static boolean isVersionSupported(String version) {
		return HADOOP_VERSION.equals(version);
	}

	/**
	 * For given Hadoop version instantiates and corresponding provider for {@link HadoopConnectingFileSystemService}.
	 * @param hadoopVersion Non-null nor empty supported Hadoop version for which the provider should be created.
	 * @param libraries Non-null URLs to required Hadoop API libraries for specified version.
	 * @return Instance of {@link HadoopConnectingFileSystemService}.
	 * @throws HadoopException If given provider class for given version cannot be found or provider cannot be
	 *         instantiated.
	 */
	public static HadoopConnectingFileSystemService createFileSystemService(String hadoopVersion, List<URL> libraries)
			throws HadoopException {
		HadoopConnectingFileSystemService fsService = loadService(hadoopVersion, libraries, FS_PROVIDER_KEY, "file system");
		return new HadoopFileSystemProxy(fsService);
	}

	/**
	 * For given Hadoop version instantiates and corresponding provider for {@link HadoopConnectingMapReduceService}.
	 * @param hadoopVersion Non-null nor empty supported Hadoop version for which the provider should be created.
	 * @param libraries Non-null URLs to required Hadoop API libraries for specified version.
	 * @return Instance of {@link HadoopConnectingMapReduceService}.
	 * @throws HadoopException If given provider class for given version cannot be found or provider cannot be
	 *         instantiated.
	 */
	public static HadoopConnectingMapReduceService createMapReduceService(String hadoopVersion, List<URL> libraries)
			throws HadoopException {
		HadoopConnectingMapReduceService mrService = loadService(hadoopVersion, libraries, MAPRED_PROVIDER_KEY, "map/reduce");
		return new HadoopMapReduceProxy(mrService);
	}

	/**
	 * For given Hadoop version instantiates and corresponding provider for {@link HadoopMapReduceInfoService}.
	 * @param hadoopVersion Non-null nor empty supported Hadoop version for which the provider should be created.
	 * @param libraries Non-null URLs to required Hadoop API libraries for specified version.
	 * @return Instance of {@link HadoopMapReduceInfoService}.
	 * @throws HadoopException If given provider class for given version cannot be found or provider cannot be
	 *         instantiated.
	 */
	public static HadoopMapReduceInfoService createMapReduceInfoService(String hadoopVersion, List<URL> libraries)
			throws HadoopException {
		HadoopMapReduceInfoService mrInfoService = loadService(hadoopVersion, libraries, MAPRED_INFO_PROVIDER_KEY, "map/reduce information");
		return new HadoopMapReduceInfoProxy(mrInfoService);
	}

	/**
	 * Actual instantiation of service providers is done by this method.
	 * 
	 * @param hadoopVersion Version of Hadoop API for which the provider is to be instantiated.
	 * @param libraries URLs to required Hadoop API libraries for the version.
	 * @param providerKey Identification of the service for which the provider is needed.
	 * @param serviceName Human readable name of the service used in exceptions messages.
	 * @return Instance of the required service.
	 * @throws HadoopException If given provider class for given version cannot be found or provider cannot be
	 *         instantiated.
	 */
	@SuppressWarnings("unchecked")
	private static <T> T loadService(String hadoopVersion, List<URL> libraries, String providerKey, String serviceName)
			throws HadoopException {
		if (hadoopVersion == null) {
			throw new NullPointerException("hadoopVersion");
		}
		if (hadoopVersion.isEmpty()) {
			throw new HadoopException("Hadoop version was not specified.", new IllegalArgumentException("hadoopVersion is empty string"));
		}
		if (libraries == null) {
			throw new NullPointerException("libraries");
		}
		if (!isVersionSupported(hadoopVersion)) {
			throw new IllegalArgumentException("Hadoop version " + hadoopVersion + " is not supported.");
		}

		String providerClassName = getClassForKey(hadoopVersion, providerKey);
		if (providerClassName.isEmpty()) {
			throw new IllegalArgumentException("providerClassName is empty string.");
		}
		LOG.debug("Loading Hadoop service '" + serviceName + "' (i.e. class " + providerClassName + ")");
		try {
			Class<?> provClass = getClassLoader(libraries).loadClass(providerClassName);
			return (T) provClass.newInstance();
		} catch (NoClassDefFoundError err) {
			classLoaderCache.remove(new HashSet<URL>(libraries));
			LOG.debug("  classloader removed from cache; classloader classpath: " + libraries);
			throw new HadoopException("Could not find required class definition. Some Hadoop libraries might be missing.", err);
		} catch (ClassNotFoundException ex) {
			throw new HadoopVersionDictionaryException("Could not load " + serviceName + " provider class '"
					+ providerClassName + "'", ex);
		} catch (InstantiationException ex) {
			throw new HadoopProviderDefinitionException("Could not instantiate " + serviceName + " provider '"
					+ providerClassName + "'. Make sure it has public non-parametric constructor.", ex);
		} catch (IllegalAccessException ex) {
			throw new HadoopProviderDefinitionException("Could not access constructor of Hadoop " + serviceName
					+ " provider '" + providerClassName + "'. Make sure there is public non-parametric constructor.", ex);
		} catch (ClassCastException ex) {
			throw new HadoopProviderDefinitionException("Type '" + providerClassName + "' does not implement "
					+ serviceName + " service as required.", ex);
		}
	}

	/**
	 * For given Hadoop API version and key specifying service returns name of corresponding provider class. TODO
	 * replace by load extension point data and decide which class to load
	 * @param hadoopVersion Version of the Hadoop API.
	 * @param key Key specifying for provider for which service is to be found.
	 * @return Name of provider class to be used for specified service and Hadoop API version
	 * @throws HadoopException If provider class cannot be found for given version.
	 */
	private static String getClassForKey(String hadoopVersion, String key) throws HadoopException {
		if (key.equals(FS_PROVIDER_KEY)) {
			return "org.jetel.hadoop.provider.filesystem.HadoopConnectingFileSystemProvider";
		} else if (key.equals(MAPRED_PROVIDER_KEY)) {
			return "org.jetel.hadoop.provider.mapreduce.HadoopConnectingMapReduceProvider";
		} else if (key.equals(MAPRED_INFO_PROVIDER_KEY)) {
			return "org.jetel.hadoop.provider.mapreduce.HadoopMapReduceInfoProvider";
		} else {
			throw new IllegalArgumentException("Unknown key: " + key);
		}
	}

	private static ClassLoader getClassLoader(List<URL> libraries) {
		if (libraries.size() == 0) {
			// Actually, this should never happen; hadoop.provider.jar is always included in the libraries
			return HadoopProvidersFactory.class.getClassLoader();
		}
		
		HashSet<URL> classPathSet = new HashSet<URL>(libraries);
		ClassLoader classLoader = classLoaderCache.get(classPathSet);
		
		if (classLoader == null) {
			classLoader = new GreedyURLClassLoader(libraries.toArray(new URL[0]), HadoopProvidersFactory.class.getClassLoader());
			classLoaderCache.put(classPathSet, classLoader);
			
			LOG.debug("  using new classloader with classpath: " + libraries);
		} else {
			LOG.debug("  using cached classloader with classpath: " + libraries + " (order may be different)");
		}
		
		return classLoader;
	}

	/**
	 * Represents generic operation, a call to a method with zero parameters and variable signature.
	 * 
	 * @param RT Method's return type.
	 * @param EX Type of exception that the method might throw.
	 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin,
	 *         a.s (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
	 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
	 * @since rel-3-4-0-M2
	 * @created 18.12.2012
	 */
	private static interface Call<RT, EX extends Exception> {
		RT execute() throws EX;
	}

	/**
	 * A proxy class for services extending {@link HadoopConnectingService}. This proxy ensures that methods of the
	 * wrapped {@link HadoopConnectingService} provider are called in correct context - that is with the right context
	 * class loader set (see {@link Thread#setContextClassLoader(ClassLoader)}).
	 * 
	 * @param E Type of the data needed carrying information needed to connect to Hadoop cluster.
	 * @param T Specific service extending {@link HadoopConnectingService} that this proxy is wrapping.
	 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin,
	 *         a.s (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
	 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
	 * @since rel-3-4-0-M2
	 * @created 18.12.2012
	 */
	private static abstract class AbstractHadoopConnectionProxy<E extends AbstractHadoopConnectionData, T extends HadoopConnectingService<E>>
			implements HadoopConnectingService<E> {
		
		private final T provider;
		
		/**
		 * Creates new proxy.
		 * @param provider Non-null wrapped provider to which all operations are delegated.
		 */
		protected AbstractHadoopConnectionProxy(T provider) {
			if (provider == null) {
				throw new NullPointerException("provider");
			}
			this.provider = provider;
		}

		/**
		 * Executes given operation (call to method) in context of the delegated provider. Context class loader is set
		 * to class loader of the delegate and restored after execution of the operation is complete.
		 * @param operation To be executed.
		 * @return Return value of the operation or <code>null</code> if return value of the operation is
		 *         <code>void</code>.
		 * @throws EX Exception throw by the operation or {@link RuntimeException} if the operation does not throw any
		 *         checked exception.
		 */
		protected final <RT, EX extends Exception> RT doInContext(final Call<RT, EX> operation) throws EX {
			ClassLoader formerContextClassLoader = Thread.currentThread().getContextClassLoader();
			ClassLoader hadoopContextClassLoader = provider.getClass().getClassLoader();
			Thread.currentThread().setContextClassLoader(hadoopContextClassLoader);
			try {
				return operation.execute();
			} catch (NoClassDefFoundError e) {
				classLoaderCache.values().remove(hadoopContextClassLoader);
				if (hadoopContextClassLoader instanceof URLClassLoader) {
					LOG.debug("  classloader removed from cache; classloader classpath: " + Arrays.toString(((URLClassLoader)hadoopContextClassLoader).getURLs()));
				} else {
					// shouldn't happen
					LOG.debug("  classloader removed from cache");
				}
				throw e;
			} finally {
				Thread.currentThread().setContextClassLoader(formerContextClassLoader);
			}
		}

		@Override
		public final void connect(final E connData, final Properties additionalProperties) throws IOException {
			doInContext(new Call<Void, IOException>() {
				@Override
				public Void execute() throws IOException {
					provider.connect(connData, additionalProperties);
					return null;
				}
			});
		}

		@Override
		public final String validateConnection() throws IOException {
			return doInContext(new Call<String, IOException>() {
				@Override
				public String execute() throws IOException {
					return provider.validateConnection();
				}
			});
		}

		@Override
		public final boolean isConnected() {
			return doInContext(new Call<Boolean, RuntimeException>() {
				@Override
				public Boolean execute() {
					return provider.isConnected();
				}
			});
		}

		@Override
		public final void close() throws IOException {
			doInContext(new Call<Void, IOException>() {
				@Override
				public Void execute() throws IOException {
					provider.close();
					return null;
				}
			});
		}

		protected final T getProvider() {
			return provider;
		}
	}

	/**
	 * A proxy class for service {@link HadoopConnectingFileSystemService}. This proxy ensures that methods of the
	 * wrapped {@link HadoopConnectingFileSystemService} provider are called in correct context - that is with the right
	 * context class loader set (see {@link Thread#setContextClassLoader(ClassLoader)}).
	 * 
	 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin,
	 *         a.s (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
	 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
	 * @since rel-3-4-0-M2
	 * @created 14.12.2012
	 */
	private static final class HadoopFileSystemProxy extends
			AbstractHadoopConnectionProxy<HadoopFileSystemConnectionData, HadoopConnectingFileSystemService> implements
			HadoopConnectingFileSystemService {

		/**
		 * Creates new proxy.
		 * @param provider Non-null wrapped provider to which all operations are delegated.
		 */
		private HadoopFileSystemProxy(HadoopConnectingFileSystemService provider) {
			super(provider);
		}

		@Override
		public String getFSMasterURLTemplate() {
			return doInContext(new Call<String, RuntimeException>() {
				@Override
				public String execute() {
					return getProvider().getFSMasterURLTemplate();
				}
			});
		}

		@Override
		public long getUsedSpace() throws IOException {
			return doInContext(new Call<Long, IOException>() {
				@Override
				public Long execute() throws IOException {
					return getProvider().getUsedSpace();
				}
			});
		}

		@Override
		public Object getDFS() {
			return doInContext(new Call<Object, RuntimeException>() {
				@Override
				public Object execute() {
					return getProvider().getDFS();
				}
			});
		}

		@Override
		public HadoopDataInput open(final URI file) throws IOException {
			return doInContext(new Call<HadoopDataInput, IOException>() {
				@Override
				public HadoopDataInput execute() throws IOException {
					return getProvider().open(file);
				}
			});
		}

		@Override
		public HadoopDataInput open(final URI file, final int bufferSize) throws IOException {
			return doInContext(new Call<HadoopDataInput, IOException>() {
				@Override
				public HadoopDataInput execute() throws IOException {
					return getProvider().open(file, bufferSize);
				}
			});
		}

		@Override
		public HadoopDataOutput create(final URI file, final boolean overwrite) throws IOException {
			return doInContext(new Call<HadoopDataOutput, IOException>() {
				@Override
				public HadoopDataOutput execute() throws IOException {
					return getProvider().create(file, overwrite);
				}
			});
		}

		@Override
		public HadoopDataOutput create(final URI file, final boolean overwrite, final int bufferSize)
				throws IOException {
			return doInContext(new Call<HadoopDataOutput, IOException>() {
				@Override
				public HadoopDataOutput execute() throws IOException {
					return getProvider().create(file, overwrite, bufferSize);
				}
			});
		}

		@Override
		public HadoopDataOutput create(final URI file, final boolean overwrite, final int bufferSize,
				final short replication, final long blockSize) throws IOException {
			return doInContext(new Call<HadoopDataOutput, IOException>() {
				@Override
				public HadoopDataOutput execute() throws IOException {
					return getProvider().create(file, overwrite, bufferSize, replication, blockSize);
				}
			});
		}
		
		@Override
		public IHadoopSequenceFileFormatter createFormatter(final String keyFieldName, final String valueFieldName,
				final boolean overwrite, final String user, final Properties hadoopProperties) throws IOException {
			return doInContext(new Call<IHadoopSequenceFileFormatter, IOException>() {
				@Override
				public IHadoopSequenceFileFormatter execute() throws IOException {
					return getProvider().createFormatter(keyFieldName, valueFieldName, overwrite, user, hadoopProperties);
				}
			});
		}

		@Override
		public IHadoopSequenceFileParser createParser(final String keyFieldName, final String valueFieldName,
				final DataRecordMetadata metadata, final String user, final Properties hadoopProperties) throws IOException {
			return doInContext(new Call<IHadoopSequenceFileParser, IOException>() {
				@Override
				public IHadoopSequenceFileParser execute() throws IOException {
					return getProvider().createParser(keyFieldName, valueFieldName, metadata, user, hadoopProperties);
				}
			});
		}

		@Override
		public HadoopDataOutput append(final URI file) throws IOException {
			return doInContext(new Call<HadoopDataOutput, IOException>() {
				@Override
				public HadoopDataOutput execute() throws IOException {
					return getProvider().append(file);
				}
			});
		}

		@Override
		public HadoopDataOutput append(final URI file, final int bufferSize) throws IOException {
			return doInContext(new Call<HadoopDataOutput, IOException>() {
				@Override
				public HadoopDataOutput execute() throws IOException {
					return getProvider().append(file, bufferSize);
				}
			});
		}

		@Override
		public boolean delete(final URI file, final boolean recursive) throws IOException {
			return doInContext(new Call<Boolean, IOException>() {
				@Override
				public Boolean execute() throws IOException {
					return getProvider().delete(file, recursive);
				}
			});
		}

		@Override
		public boolean exists(final URI file) throws IOException {
			return doInContext(new Call<Boolean, IOException>() {
				@Override
				public Boolean execute() throws IOException {
					return getProvider().exists(file);
				}
			});
		}

		@Override
		public boolean mkdir(final URI file) throws IOException {
			return doInContext(new Call<Boolean, IOException>() {
				@Override
				public Boolean execute() throws IOException {
					return getProvider().mkdir(file);
				}
			});
		}

		@Override
		public boolean rename(final URI src, final URI dst) throws IOException {
			return doInContext(new Call<Boolean, IOException>() {
				@Override
				public Boolean execute() throws IOException {
					return getProvider().rename(src, dst);
				}
			});
		}

		@Override
		public HadoopFileStatus[] listStatus(final URI path) throws IOException {
			return doInContext(new Call<HadoopFileStatus[], IOException>() {
				@Override
				public HadoopFileStatus[] execute() throws IOException {
					return getProvider().listStatus(path);
				}
			});
		}

		@Override
		public HadoopFileStatus getStatus(final URI path) throws IOException {
			return doInContext(new Call<HadoopFileStatus, IOException>() {
				@Override
				public HadoopFileStatus execute() throws IOException {
					return getProvider().getStatus(path);
				}
			});
		}

		@Override
		public HadoopFileStatus getExtendedStatus(final URI path) throws IOException {
			return doInContext(new Call<HadoopFileStatus, IOException>() {
				@Override
				public HadoopFileStatus execute() throws IOException {
					return getProvider().getExtendedStatus(path);
				}
			});
		}

		@Override
		public boolean createNewFile(final URI file) throws IOException {
			return doInContext(new Call<Boolean, IOException>() {
				@Override
				public Boolean execute() throws IOException {
					return getProvider().createNewFile(file);
				}
			});
		}

		@Override
		public HadoopFileStatus[] globStatus(final String glob) throws IOException {
			return doInContext(new Call<HadoopFileStatus[], IOException>() {
				@Override
				public HadoopFileStatus[] execute() throws IOException {
					return getProvider().globStatus(glob);
				}
			});
		}

		@Override
		public void setLastModified(final URI path, final long lastModified) throws IOException {
			doInContext(new Call<Void, IOException>() {
				@Override
				public Void execute() throws IOException {
					getProvider().setLastModified(path, lastModified);
					return null;
				}
			});
		}
		
	}

	/**
	 * A proxy class for service {@link HadoopConnectingMapReduceService}. This proxy ensures that methods of the
	 * wrapped {@link HadoopConnectingMapReduceService} provider are called in correct context - that is with the right
	 * context class loader set (see {@link Thread#setContextClassLoader(ClassLoader)}).
	 * 
	 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin,
	 *         a.s (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
	 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
	 * @since rel-3-4-0-M2
	 * @created 18.12.2012
	 */
	private static class HadoopMapReduceProxy extends
			AbstractHadoopConnectionProxy<HadoopMapReduceConnectionData, HadoopConnectingMapReduceService> implements
			HadoopConnectingMapReduceService {

		/**
		 * Creates new proxy.
		 * @param provider Non-null wrapped provider to which all operations are delegated.
		 */
		protected HadoopMapReduceProxy(HadoopConnectingMapReduceService provider) {
			super(provider);
		}

		@Override
		public HadoopJobReporter sendJob(final HadoopMapReduceJob job, final Properties additionalJobProperties)
				throws IOException {
			return doInContext(new Call<HadoopJobReporter, IOException>() {
				@Override
				public HadoopJobReporter execute() throws IOException {
					return getProvider().sendJob(job, additionalJobProperties);
				}
			});
		}
	}

	/**
	 * A proxy class for service {@link HadoopMapReduceInfoService}. This proxy ensures that methods of the wrapped
	 * {@link HadoopMapReduceInfoService} provider are called in correct context - that is with the right context class
	 * loader set (see {@link Thread#setContextClassLoader(ClassLoader)}).
	 * 
	 * @author Rastislav Mirek &lt;<a href="mailto:rmirek@mail.muni.cz">rmirek@mail.muni.cz</a>&gt</br> &#169; Javlin,
	 *         a.s (<a href="http://www.javlin.eu">www.javlin.eu</a>) &lt;<a
	 *         href="mailto:info@cloveretl.com">info@cloveretl.com</a>&gt
	 * @since rel-3-4-0-M2
	 * @created 18.12.2012
	 */
	private static final class HadoopMapReduceInfoProxy implements HadoopMapReduceInfoService {
		private HadoopMapReduceInfoService provider;

		/**
		 * Creates new proxy.
		 * @param provider Non-null wrapped provider to which all operations are delegated.
		 */
		private HadoopMapReduceInfoProxy(HadoopMapReduceInfoService provider) {
			if (provider == null) {
				throw new NullPointerException("provider");
			}
			this.provider = provider;
		}

		@Override
		public List<HadoopCounterGroup> getCounterGroups() {
			ClassLoader formerContextClassLoader = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(provider.getClass().getClassLoader());
			try {
				return provider.getCounterGroups();
			} finally {
				Thread.currentThread().setContextClassLoader(formerContextClassLoader);
			}
		}
	}
}
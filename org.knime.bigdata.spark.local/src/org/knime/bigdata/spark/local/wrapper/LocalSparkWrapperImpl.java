package org.knime.bigdata.spark.local.wrapper;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.local.context.LocalSparkSerializationUtil;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SimpleSparkJob;
import org.knime.bigdata.spark2_2.api.SparkJob;
import org.knime.bigdata.spark2_2.api.SparkJobWithFiles;

@SparkClass
public class LocalSparkWrapperImpl implements LocalSparkWrapper, NamedObjects {

	// private final static Logger LOG = Logger.getLogger(LocalSparkWrapperImpl.class);
	
	private final static String SPARK_APP_NAME = "spark.app.name";
	
	private final static String SPARK_MASTER = "spark.master";
	
	private final static String SPARK_LOCAL_DIR = "spark.local.dir";
	
	private String m_derbyUrl;
	
	private int m_hiveserverPort = -1;

	private final Map<String, Object> m_namedObjects = new HashMap<>();

	private SparkSession m_sparkSession;
	
	/**
	 * Parent directory for temporary data.
	 */
	private File m_sparkTmpDir;
	
	/**
	 * Subdirectory of {@link #m_sparkTmpDir} that is used to store copies of
	 * the input files for {@link SparkJobWithFiles}. This is necessary because
	 * these files are managed by KNIME nodes and may be deleted when a node is
	 * reset. Since this can cause FileNotFoundExceptions in local Spark, we
	 * ensure that Spark works on independent copies.
	 */
	private File m_jobInputFileCopyDir;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map<String, Object> runJob(Map<String, Object> localSparkInputMap) {
		// we need to replace the current context class loader (which comes from OSGI)
		// with the spark class loader, otherwise Java's ServiceLoader frame does not
		// work properly which breaks Spark's DataSource API
		final ClassLoader origContextClassLoader = swapContextClassLoader();
		LocalSparkJobOutput toReturn;

		try {
			final LocalSparkJobInput localSparkInput = LocalSparkJobInput.createFromMap(LocalSparkSerializationUtil
					.deserializeFromPlainJavaTypes(localSparkInputMap, getClass().getClassLoader()));
			final JobInput jobInput = localSparkInput.getSparkJobInput();

			ensureNamedInputObjectsExist(jobInput);
			ensureNamedOutputObjectsDoNotExist(jobInput);

			// The input files are managed by KNIME nodes and may be deleted
			// when a node is reset. Since this can cause FileNotFoundExceptions
			// in local Spark, we ensure that Spark works on independent copies
			// of the input files.
			List<File> inputFileCopies = copyInputFiles(localSparkInput);

			Object sparkJob = getClass().getClassLoader().loadClass(localSparkInput.getSparkJobClass()).newInstance();

			if (sparkJob instanceof SparkJob) {
				toReturn = LocalSparkJobOutput
						.success(((SparkJob) sparkJob).runJob(m_sparkSession.sparkContext(), jobInput, this));
			} else if (sparkJob instanceof SparkJobWithFiles) {
				toReturn = LocalSparkJobOutput.success(((SparkJobWithFiles) sparkJob)
						.runJob(m_sparkSession.sparkContext(), jobInput, inputFileCopies, this));
			} else {
				((SimpleSparkJob) sparkJob).runJob(m_sparkSession.sparkContext(), jobInput, this);
				toReturn = LocalSparkJobOutput.success();
			}
		} catch (KNIMESparkException e) {
			toReturn = LocalSparkJobOutput.failure(e);
		} catch (Throwable t) {
			toReturn = LocalSparkJobOutput
					.failure(new KNIMESparkException("Failed to execute Spark job: " + t.getMessage(), t));
		} finally {
			Thread.currentThread().setContextClassLoader(origContextClassLoader);
		}

		return LocalSparkSerializationUtil.serializeToPlainJavaTypes(toReturn.getInternalMap());
	}

	private List<File> copyInputFiles(final LocalSparkJobInput jsInput) throws KNIMESparkException, IOException {
		List<File> inputFileCopies = new LinkedList<>();

		for (String pathToFile : jsInput.getFiles()) {
			final File inputFile = new File(pathToFile);
			if (inputFile.canRead()) {
				inputFileCopies.add(copyJobInputFile(inputFile));
			} else {
				throw new KNIMESparkException("Cannot read job input file: " + pathToFile);
			}
		}

		return inputFileCopies;
	}

	private File copyJobInputFile(File inputFile) throws IOException {
		final File inputFileCopy = Files.createTempFile(m_jobInputFileCopyDir.toPath(), null, inputFile.getName()).toFile();
		Files.copy(inputFile.toPath(), inputFileCopy.toPath(), StandardCopyOption.REPLACE_EXISTING);
		
		return inputFileCopy;
	}

	private void ensureNamedOutputObjectsDoNotExist(final JobInput input) throws KNIMESparkException {
		// validate named output objects do not exist
		for (String namedOutputObject : input.getNamedOutputObjects()) {
			if (validateNamedObject(namedOutputObject)) {
				throw new KNIMESparkException(
						"Spark RDD/DataFrame to create already exists. Please reset all preceding nodes and reexecute.");
			}
		}
	}

	private void ensureNamedInputObjectsExist(final JobInput input) throws KNIMESparkException {
		for (String namedInputObject : input.getNamedInputObjects()) {
			if (!validateNamedObject(namedInputObject)) {
				throw new KNIMESparkException(
						"Missing input Spark RDD/DataFrame. Please reset all preceding nodes and reexecute.");
			}
		}
	}

	@Override
	public void addDataFrame(String key, Dataset<Row> dataset) {
		synchronized (m_namedObjects) {
			m_namedObjects.put(key, dataset);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Dataset<Row> getDataFrame(String key) {
		synchronized (m_namedObjects) {
			return (Dataset<Row>) m_namedObjects.get(key);
		}
	}

	@Override
	public JavaRDD<Row> getJavaRdd(String key) {
		synchronized (m_namedObjects) {
			return getDataFrame(key).toJavaRDD();
		}
	}

	@Override
	public boolean validateNamedObject(String key) {
		synchronized (m_namedObjects) {
			return m_namedObjects.containsKey(key);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deleteNamedDataFrame(String key) {
		synchronized (m_namedObjects) {
			Object removed = m_namedObjects.remove(key);
			if (removed != null) {
				((Dataset<Row>) removed).unpersist();
			}
		}
	}

	@Override
	public Set<String> getNamedObjects() {
		synchronized (m_namedObjects) {
			return new HashSet<String>(m_namedObjects.keySet());
		}
	}

	@Override
	public synchronized void openSparkContext(String name, int workerThreads, Map<String, String> userSparkConf,
			boolean enableHiveSupport, boolean startThriftserver, String hiveDataFolder) throws KNIMESparkException {
		
		// we need to replace the current context class loader (which comes from OSGI)
		// with the spark class loader, otherwise Java's ServiceLoader frame does not
		// work properly which breaks Spark's DataSource API
		final ClassLoader origContextClassLoader = swapContextClassLoader();
		
		try {
			
			final SparkConf sparkConf = new SparkConf(false);
			
			final Map<String, String> filteredUserSparkConf = filterUserSparkConfMap(userSparkConf);
			
			initSparkTmpDir();
			initJobInputFileCopyDir();
			
			configureSparkLocalDir(filteredUserSparkConf, sparkConf);
			
			if (enableHiveSupport) {
				configureHiveSupport(filteredUserSparkConf, sparkConf, hiveDataFolder);

				if (startThriftserver) {
					configureThriftserver(sparkConf);
				}
			}

			// it is important to do this last, because it allows the user to overwrite some
			// defaults we are assuming.
			for (String userKey : filteredUserSparkConf.keySet()) {
				sparkConf.set(userKey, filteredUserSparkConf.get(userKey));
			}
			
			m_sparkSession = SparkSession
				.builder()
				.appName(String.format("Local Spark (%s)", name))
				.master(String.format("local[%d]", workerThreads))
				.config("spark.logConf", "true")
				.config("spark.kryo.unsafe", "true")
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.config(sparkConf)
				.getOrCreate();
			
			if (startThriftserver) {
				HiveThriftServer2.startWithContext(m_sparkSession.sqlContext());
			}
		} catch (IOException e) {
			throw new KNIMESparkException(e);
		} finally {
			Thread.currentThread().setContextClassLoader(origContextClassLoader);
		}
	}

	private void initJobInputFileCopyDir() throws IOException {
		m_jobInputFileCopyDir = new File(m_sparkTmpDir, "spark_filecopy_dir");
		if (!m_jobInputFileCopyDir.mkdir()) {
			throw new IOException("Could not create temporary file copy directory for local Spark.");
		}
	}

	private void configureThriftserver(SparkConf sparkConf) throws IOException {
		m_hiveserverPort = findRandomFreePort();
		
		sparkConf.set("hive.server2.thrift.port", Integer.toString(m_hiveserverPort));
		sparkConf.set("hive.server2.thrift.bind.host", "localhost");
	}

	private int findRandomFreePort() throws IOException {
		int freePort;
		try (ServerSocket s = new ServerSocket(0)) {
			freePort = s.getLocalPort();
		}
		return freePort;
	}

	private void configureSparkLocalDir(final Map<String, String> userSparkConf, final SparkConf sparkConf)
			throws IOException {
		final File sparkLocalDir;

		if (userSparkConf.containsKey(SPARK_LOCAL_DIR)) {
			sparkLocalDir = new File(userSparkConf.get(SPARK_LOCAL_DIR));
		} else {
			sparkLocalDir = new File(m_sparkTmpDir, "spark_local_dir");
			if (!sparkLocalDir.mkdir()) {
				throw new IOException("Could not create temporary directory for spark.local.dir");
			}
		}

		sparkConf.set(SPARK_LOCAL_DIR, sparkLocalDir.getAbsolutePath());
	}
	
	private void configureHiveSupport(Map<String, String> filteredUserSparkConf, SparkConf sparkConf,
			String hiveDataFolder) throws IOException {

		final File hiveParentDir;
		if (hiveDataFolder != null) {
			hiveParentDir = new File(hiveDataFolder);

			if (!hiveParentDir.exists()) {
				throw new IllegalArgumentException(
						String.format("Hive data folder %s does not exist. Please create it first.",
								hiveDataFolder));
			}
			
			ensureWritableDirectory(hiveParentDir, "Hive data folder");

			final File derbyMetastoreDB = new File(hiveParentDir, "metastore_db");
			if (derbyMetastoreDB.exists()) {
				ensureWritableDirectory(derbyMetastoreDB, "Metastore DB folder");
			}
			m_derbyUrl = String.format("jdbc:derby:%s", derbyMetastoreDB.getCanonicalPath());
		} else {
			hiveParentDir = m_sparkTmpDir;
			m_derbyUrl = "jdbc:derby:memory:" + UUID.randomUUID().toString();
		}

		final File warehouseDir = new File(hiveParentDir, "warehouse");
		if (warehouseDir.exists()) {
			ensureWritableDirectory(warehouseDir, "Hive warehouse");
		} else if (!warehouseDir.mkdir()) {
			throw new IOException("Could not create Hive warehouse directory at " + warehouseDir.getAbsolutePath());
		}

		final File hiveOperationLogsDir = new File(hiveParentDir, "hive_operation_logs");
		if (hiveOperationLogsDir.exists()) {
			ensureWritableDirectory(hiveOperationLogsDir, "Hiveserver operations log");
		} else if (!hiveOperationLogsDir.mkdir()) {
			throw new IOException("Could not create directory for Hiveserver operations log at " + hiveOperationLogsDir.getAbsolutePath());
		}
		
		final File hiveScratchDir = new File(m_sparkTmpDir, "hive_scratch");
		if (hiveScratchDir.exists()) {
			ensureWritableDirectory(hiveScratchDir, "Hive scratch");
		}
		
		sparkConf.set("javax.jdo.option.ConnectionURL", m_derbyUrl + ";create=true");
		sparkConf.set("spark.sql.warehouse.dir", warehouseDir.getCanonicalPath());
		sparkConf.set("hive.server2.logging.operation.log.location", hiveOperationLogsDir.getCanonicalPath());
		sparkConf.set("spark.sql.catalogImplementation", "hive");
		sparkConf.set("hive.exec.scratchdir", hiveScratchDir.getCanonicalPath());
	}
	
	private void ensureWritableDirectory(final File maybeDir, final String errorMsgName) throws IOException {
		if (!maybeDir.isDirectory()) {
			throw new IOException(
					String.format("%s at %s exists but is not a directory.", errorMsgName, maybeDir.getAbsolutePath()));
		} else if (!maybeDir.canWrite()) {
			throw new IOException(
					String.format("%s at %s is write-protected. Please change file permissions accordingly.",
							errorMsgName, maybeDir.getAbsolutePath()));
		}
	}

	private void initSparkTmpDir() throws IOException {
		// create a temporary directory for Spark that gets deleted during JVM
		// shutdown and/or destroy()
		m_sparkTmpDir = Files.createTempDirectory("knime_localspark_").toFile();
		deleteRecursivelyOnExit(m_sparkTmpDir);
	}

	private Map<String, String> filterUserSparkConfMap(Map<String, String> sparkConfMap) {
		final Map<String, String> filteredMap = new HashMap<>(sparkConfMap);
		filteredMap.remove(SPARK_APP_NAME);
		filteredMap.remove(SPARK_MASTER);

		// not filtering SPARK_LOCAL_DIR because the user *may* want to set this

		return filteredMap;
	}

	private void deleteRecursivelyOnExit(final File tmpData) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				FileUtils.deleteQuietly(tmpData);
			}
		});
	}

	private ClassLoader swapContextClassLoader() {
		final ClassLoader contextClassLoaderBackup = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
		return contextClassLoaderBackup;
	}

	@Override
	public synchronized void destroy() throws KNIMESparkException {
		m_sparkSession.close();
		m_sparkSession = null;
		try {
			// shut down the entire derby system
			DriverManager.getConnection("jdbc:derby:;shutdown=true");
		} catch (SQLException e) {
		}
		FileUtils.deleteQuietly(m_sparkTmpDir);
	}

	@Override
	public void deleteNamedObjects(Set<String> namedObjects) throws KNIMESparkException {
		for (String namedObjectId : namedObjects) {
			deleteNamedDataFrame(namedObjectId);
		}
	}

	@Override
	public String getSparkWebUIUrl() {
		return m_sparkSession.sparkContext().uiWebUrl().get();
	}

	@Override
	public int getHiveserverPort() {
		return m_hiveserverPort;
	}

	@Override
	public <T> void add(String key, T obj) {
		synchronized (m_namedObjects) {
			m_namedObjects.put(key, obj);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(String key) {
		synchronized (m_namedObjects) {
			return (T) m_namedObjects.get(key);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T delete(String key) {
		synchronized (m_namedObjects) {
			return (T) m_namedObjects.remove(key);
		}
	}
}

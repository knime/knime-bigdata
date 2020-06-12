package org.knime.bigdata.spark.local.wrapper;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Shell;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.context.namedobjects.SparkDataObjectStatistic;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.local.context.LocalSparkSerializationUtil;
import org.knime.bigdata.spark.local.hadoop.LocalFileSystemHiveTempWrapper;
import org.knime.bigdata.spark2_4.api.NamedObjects;
import org.knime.bigdata.spark2_4.api.SimpleSparkJob;
import org.knime.bigdata.spark2_4.api.SparkJob;
import org.knime.bigdata.spark2_4.api.SparkJobWithFiles;

import scala.Option;

/**
 * Implementation of {@link LocalSparkWrapper} and {@link NamedObjects}. Objects of this class hold and manage an actual
 * SparkContext and run jobs on it.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class LocalSparkWrapperImpl implements LocalSparkWrapper, NamedObjects {

	// private final static Logger LOG = Logger.getLogger(LocalSparkWrapperImpl.class);
	
	private final static String SPARK_APP_NAME = "spark.app.name";
	
	private final static String SPARK_MASTER = "spark.master";
	
	private final static String SPARK_LOCAL_DIR = "spark.local.dir";
	
	private final static String SPARK_DRIVER_HOST = "spark.driver.host";
	
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
    public Map<String, Object> runJob(final Map<String, Object> localSparkInputMap, final String jobGroupId)
        throws InterruptedException {
	    
		// we need to replace the current context class loader (which comes from OSGI)
		// with the spark class loader, otherwise Java's ServiceLoader frame does not
		// work properly which breaks Spark's DataSource API
		final ClassLoader origContextClassLoader = swapContextClassLoader();
		WrapperJobOutput toReturn;

		try {
			final LocalSparkJobInput localSparkInput = new LocalSparkJobInput(); 
			LocalSparkSerializationUtil.deserializeFromPlainJavaTypes(localSparkInputMap, getClass().getClassLoader(), localSparkInput);
			
			final JobInput jobInput = localSparkInput.getSparkJobInput();

			Object sparkJob = getClass().getClassLoader().loadClass(localSparkInput.getSparkJobClass()).newInstance();

			markJobGroupId(jobGroupId, sparkJob.getClass().getSimpleName());
			
			ensureNamedInputObjectsExist(jobInput);
			ensureNamedOutputObjectsDoNotExist(jobInput);

			// The input files are managed by KNIME nodes and may be deleted
			// when a node is reset. Since this can cause FileNotFoundExceptions
			// in local Spark, we ensure that Spark works on independent copies
			// of the input files.
			List<File> inputFileCopies = copyInputFiles(localSparkInput);

			if (sparkJob instanceof SparkJob) {
				toReturn = WrapperJobOutput
						.success(((SparkJob) sparkJob).runJob(m_sparkSession.sparkContext(), jobInput, this));
			} else if (sparkJob instanceof SparkJobWithFiles) {
				toReturn = WrapperJobOutput.success(((SparkJobWithFiles) sparkJob)
						.runJob(m_sparkSession.sparkContext(), jobInput, inputFileCopies, this));
			} else {
				((SimpleSparkJob) sparkJob).runJob(m_sparkSession.sparkContext(), jobInput, this);
				toReturn = WrapperJobOutput.success();
			}

            addDataFrameNumPartitions(jobInput.getNamedOutputObjects(), toReturn, this);

		} catch (InterruptedException e) {
		    // catch and rethrow to prevent the InterruptedExceptions from being wrapped
		    // into the job output, as it is thrown during cancellation
		    throw e;
		} catch (KNIMESparkException e) {
			toReturn = WrapperJobOutput.failure(e);
		} catch (Throwable t) {
			toReturn = WrapperJobOutput.failure(new KNIMESparkException(t));
		} finally {
			Thread.currentThread().setContextClassLoader(origContextClassLoader);
		}

		return LocalSparkSerializationUtil.serializeToPlainJavaTypes(toReturn);
	}

    /**
     * Add number of partitions of output objects to job result.
     */
    private static void addDataFrameNumPartitions(final List<String> outputObjects, final WrapperJobOutput jobOutput,
            final NamedObjects namedObjects) {

        if (!outputObjects.isEmpty()) {
            for (int i = 0; i < outputObjects.size(); i++) {
                final String key = outputObjects.get(i);
                final Dataset<Row> df = namedObjects.getDataFrame(key);

                if (df != null) {
                    final NamedObjectStatistics stat =
                        new SparkDataObjectStatistic(((Dataset<?>)df).rdd().getNumPartitions());
                    jobOutput.setNamedObjectStatistic(key, stat);
                }
            }
        }
    }

    /**
     * Thread-safe method (against {@link #cancelJob(String)} to check for cancellation and set the current job group id
     * if not cancelled.
     * 
     * @param jobGroupId
     * @param jobDescription
     * @throws InterruptedException
     */
	private synchronized void markJobGroupId(final String jobGroupId, final String jobDescription) throws InterruptedException {
	    if (Thread.interrupted()) {
	        throw new InterruptedException();
	    }
        m_sparkSession.sparkContext().setJobGroup(jobGroupId, jobDescription, true);
    }
	
    @Override
    public synchronized void cancelJob(String jobGroupId) {
        m_sparkSession.sparkContext().cancelJobGroup(jobGroupId);
    }

    private List<File> copyInputFiles(final LocalSparkJobInput localSparkInput) throws KNIMESparkException, IOException {
		List<File> inputFileCopies = new LinkedList<>();

		for (Path inputFile : localSparkInput.getFiles()) {
			if (Files.isReadable(inputFile)) {
				inputFileCopies.add(copyJobInputFile(inputFile));
			} else {
				throw new KNIMESparkException("Cannot read job input file: " + inputFile.toString() );
			}
		}

		return inputFileCopies;
	}

	private File copyJobInputFile(Path inputFile) throws IOException {
		final Path inputFileCopy = Files.createTempFile(m_jobInputFileCopyDir.toPath(), null, inputFile.getFileName().toString());
		Files.copy(inputFile, inputFileCopy, StandardCopyOption.REPLACE_EXISTING);
		
		return inputFileCopy.toFile();
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
        boolean enableHiveSupport, boolean startThriftserver, int thriftserverPort, String hiveDataFolder)
        throws KNIMESparkException {
		
		// we need to replace the current context class loader (which comes from OSGI)
		// with the spark class loader, otherwise Java's ServiceLoader frame does not
		// work properly which breaks Spark's DataSource API
		final ClassLoader origContextClassLoader = swapContextClassLoader();
		
		try {
			
			final SparkConf sparkConf = new SparkConf(false);
			final String pythonPath = getPythonPath(sparkConf);
			final Map<String, String> filteredUserSparkConf = filterUserSparkConfMap(userSparkConf);
			
			initSparkTmpDir();
			initJobInputFileCopyDir();
			
			configureSparkLocalDir(filteredUserSparkConf, sparkConf);
			
			if (enableHiveSupport) {
				configureHiveSupport(sparkConf, hiveDataFolder);

				if (startThriftserver) {
					configureThriftserver(sparkConf, thriftserverPort);
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
				.config("spark.knime.knosp.localBigDataEnv", "true")
				.config("spark.knime.pythonpath", pythonPath)
				.config("spark.logConf", "true")
				.config("spark.kryo.unsafe", "true")
				.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.config(SPARK_DRIVER_HOST, "localhost")
				.config("spark.executorEnv.PYTHONPATH", pythonPath.replace(',', File.pathSeparatorChar))
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

    private static String getPythonPath(SparkConf conf) throws IOException {
        final Option<String> jar = SparkContext.jarOfObject(conf);
        if (jar.isDefined()) {
            Path jarPath = Paths.get(URI.create("file:" + jar.get()));
            try(Stream<Path> files = Files.list(jarPath.getParent())) {
                return files
                    .map(Path::toString)
                    .filter(p -> p.endsWith(".zip"))
                    .collect(Collectors.joining(","));
            }
        } else {
            return "";
        }
    }
	
	private void initJobInputFileCopyDir() throws IOException {
		m_jobInputFileCopyDir = new File(m_sparkTmpDir, "spark_filecopy_dir");
		if (!m_jobInputFileCopyDir.mkdir()) {
			throw new IOException("Could not create temporary file copy directory for local Spark.");
		}
	}

    private void configureThriftserver(final SparkConf sparkConf, final int thriftserverPort) throws IOException {
        if (thriftserverPort == -1) {
            m_hiveserverPort = findRandomFreePort();
        } else {
            m_hiveserverPort = thriftserverPort;
        }

        sparkConf.set("hive.server2.thrift.port", Integer.toString(m_hiveserverPort));
        sparkConf.set("hive.server2.thrift.bind.host", "localhost");
        sparkConf.set("hive.server2.session.check.interval", "2h"); // default: 6h
        sparkConf.set("hive.server2.idle.session.timeout", "2h");// default: 7d
    }

	private static int findRandomFreePort() throws IOException {
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
	
	private void configureHiveSupport(SparkConf sparkConf, String hiveDataFolder) throws IOException {

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
		sparkConf.set("spark.sql.catalogImplementation", "hive");

		// add local file system wrapper to fake permissions of hive temporary directories
		sparkConf.set("spark.hadoop.fs." + LocalFileSystemHiveTempWrapper.SCHEME + ".impl",
			LocalFileSystemHiveTempWrapper.class.getName());
		sparkConf.set("hive.exec.scratchdir", getDummyPermissionFileSystemUri(hiveScratchDir));
		sparkConf.set("spark.sql.warehouse.dir", getDummyPermissionFileSystemUri(warehouseDir));
		sparkConf.set("hive.server2.logging.operation.log.location",
			getDummyPermissionFileSystemUri(hiveOperationLogsDir));

		// To shutdown the derby system on destroy, the Derby driver needs to be loaded
		// from the same (shared) class loader in the Hiveserver
		// Note: This fix can be removed in Spark 2.4: https://issues.apache.org/jira/browse/SPARK-23831
		sparkConf.set("spark.sql.hive.metastore.sharedPrefixes", "org.apache.derby.");
	}
	
	private static void ensureWritableDirectory(final File maybeDir, final String errorMsgName) throws IOException {
		if (!maybeDir.isDirectory()) {
			throw new IOException(
					String.format("%s at %s exists but is not a directory.", errorMsgName, maybeDir.getAbsolutePath()));
		} else if (!maybeDir.canWrite()) {
			throw new IOException(
					String.format("%s at %s is write-protected. Please change file permissions accordingly.",
							errorMsgName, maybeDir.getAbsolutePath()));
		}
	}

	private static String getDummyPermissionFileSystemUri(final File hiveScratchDir) {
		return LocalFileSystemHiveTempWrapper.SCHEME + "://" + hiveScratchDir.toURI().getPath();
	}

	private void initSparkTmpDir() throws IOException {
		// create a temporary directory for Spark that gets deleted during JVM
		// shutdown and/or destroy()
		m_sparkTmpDir = Files.createTempDirectory("knime_localspark_").toFile();
		deleteRecursivelyOnExit(m_sparkTmpDir);
	}

	private static Map<String, String> filterUserSparkConfMap(Map<String, String> sparkConfMap) {
		final Map<String, String> filteredMap = new HashMap<>(sparkConfMap);
		filteredMap.remove(SPARK_APP_NAME);
		filteredMap.remove(SPARK_MASTER);
		filteredMap.remove(SPARK_DRIVER_HOST);

		// not filtering SPARK_LOCAL_DIR because the user *may* want to set this

		return filteredMap;
	}

	private static void deleteRecursivelyOnExit(final File tmpData) {
	    // ensure that FileUtils class is loaded before class loader shutdown:
	    FileUtils.class.toString();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteQuietly(tmpData)));
	}

	private ClassLoader swapContextClassLoader() {
		final ClassLoader contextClassLoaderBackup = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
		return contextClassLoaderBackup;
	}

	@Override
	public synchronized void destroy() throws KNIMESparkException {
	    if (m_sparkSession != null) {
    		m_sparkSession.close();
    		m_sparkSession = null;
    		try {
    			// shut down the entire derby system
    			DriverManager.getConnection("jdbc:derby:;shutdown=true");
    		} catch (SQLException e) {
    		}
	    }
		FileUtils.deleteQuietly(m_sparkTmpDir);
	}

	@Override
	public void deleteNamedObjects(Set<String> namedObjects) {
		for (String namedObjectId : namedObjects) {
		    final Object namedObject = m_namedObjects.get(namedObjectId);
		    if (namedObject != null && namedObject instanceof Dataset) {
		        deleteNamedDataFrame(namedObjectId);
		    } else {
		        m_namedObjects.remove(namedObjectId);
		    }
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
    public <T> T delete(String namedObjectId) {
        synchronized (m_namedObjects) {
            final T namedObject = (T) m_namedObjects.get(namedObjectId);
            if (namedObject != null && namedObject instanceof Dataset) {
                deleteNamedDataFrame(namedObjectId);
            } else {
                m_namedObjects.remove(namedObjectId);
            }
            return namedObject;
        }
    }
}

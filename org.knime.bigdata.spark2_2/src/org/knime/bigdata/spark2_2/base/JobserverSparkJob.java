package org.knime.bigdata.spark2_2.base;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.jobserver.JobserverJobInput;
import org.knime.bigdata.spark.core.jobserver.JobserverJobOutput;
import org.knime.bigdata.spark.core.jobserver.TypesafeConfigSerializationUtils;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SimpleSparkJob;
import org.knime.bigdata.spark2_2.api.SparkJob;
import org.knime.bigdata.spark2_2.api.SparkJobWithFiles;

import com.knime.bigdata.spark.jobserver.server.KNIMESparkJob;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import spark.jobserver.api.DataFileCache;
import spark.jobserver.api.JobEnvironment;

/**
 * Job class binding to Spark Jobserver 0.7-release line. This class translates Jobserver's Scala job interface to Java.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @author Sascha Wolke, KNIME GmbH
 * @author Nico Siebert, KNIME GmbH
 */
@SparkClass
public class JobserverSparkJob extends KNIMESparkJob {

    private static final NamedObjects namedObjects = new NamedObjectsImpl();

    /** Empty deserialization constructor */
    public JobserverSparkJob() {
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public String runJob(final SparkContext sparkContext, final JobEnvironment runtime, final Config config) {
        InterceptingAppender appender = null;
        JobserverJobOutput toReturn;

        try {
            final JobserverJobInput jsInput = JobserverJobInput.createFromMap(TypesafeConfigSerializationUtils
                .deserializeFromTypesafeConfig(config, this.getClass().getClassLoader()));

            final JobInput input = jsInput.getSparkJobInput();

            ensureNamedInputObjectsExist(input);
            ensureNamedOutputObjectsDoNotExist(input);
            List<File> inputFiles = validateInputFiles(runtime, jsInput);

            Object sparkJob = getClass().getClassLoader().loadClass(jsInput.getSparkJobClass()).newInstance();

            // FIXME this is quite probably broken when multiple jobs run at the same time. in
            // this case we may get log messages from other jobs within the same context.
            appender = new InterceptingAppender(jsInput.getLog4jLogLevel());
            Logger.getRootLogger().addAppender(appender);

            if (sparkJob instanceof SparkJob) {
                toReturn = JobserverJobOutput.success(((SparkJob) sparkJob).runJob(sparkContext, input, namedObjects));
            } else if (sparkJob instanceof SparkJobWithFiles){
                toReturn = JobserverJobOutput.success(((SparkJobWithFiles) sparkJob).runJob(sparkContext, input, inputFiles, namedObjects));
            } else {
                ((SimpleSparkJob) sparkJob).runJob(sparkContext, input, namedObjects);
                toReturn = JobserverJobOutput.success();
            }
        } catch (KNIMESparkException e) {
            toReturn = JobserverJobOutput.failure(e);
        } catch (Throwable t) {
            toReturn = JobserverJobOutput.failure(new KNIMESparkException("Failed to execute Spark job: " + t.getMessage(), t));
        }

        if (appender != null) {
            Logger.getRootLogger().removeAppender(appender);
            toReturn = toReturn.withLogMessages(appender.getLogMessages());
        }

        return TypesafeConfigSerializationUtils.serializeToTypesafeConfig(toReturn.getInternalMap()).root()
            .render(ConfigRenderOptions.concise());
    }

    private List<File> validateInputFiles(final JobEnvironment runtime, final JobserverJobInput jsInput) throws KNIMESparkException {
        List<File> inputFiles = new LinkedList<>();

        if (runtime instanceof DataFileCache) {
            final DataFileCache fileCache = (DataFileCache) runtime;

            for (String pathToFile : jsInput.getFiles()) {
                try {
                    File inputFile = fileCache.getDataFile(pathToFile);

                    if (inputFile.canRead()) {
                        inputFiles.add(inputFile);
                    } else {
                        throw new KNIMESparkException("Cannot read input file on jobserver: " + pathToFile);
                    }

                } catch(IOException e) {
                    throw new KNIMESparkException("Cannot read input file on jobserver: " + pathToFile, e);
                }
            }

        } else {
            for (String pathToFile : jsInput.getFiles()) {
                File inputFile = new File(pathToFile);
                if (inputFile.canRead()) {
                    inputFiles.add(inputFile);
                } else {
                    throw new KNIMESparkException("Cannot read input file on jobserver: " + pathToFile);
                }
            }
        }

        return inputFiles;
    }

    private void ensureNamedOutputObjectsDoNotExist(final JobInput input) throws KNIMESparkException {
        // validate named output objects do not exist
        for (String namedOutputObject : input.getNamedOutputObjects()) {
            if (namedObjects.validateNamedObject(namedOutputObject)) {
                throw new KNIMESparkException(
                    "Spark RDD/DataFrame to create already exists. Please reset all preceding nodes and reexecute.");
            }
        }
    }

    private void ensureNamedInputObjectsExist(final JobInput input) throws KNIMESparkException {
        for (String namedInputObject : input.getNamedInputObjects()) {
            if (!namedObjects.validateNamedObject(namedInputObject)) {
                throw new KNIMESparkException(
                    "Missing input Spark RDD/DataFrame. Please reset all preceding nodes and reexecute.");
            }
        }
    }
}

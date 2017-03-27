package com.knime.bigdata.spark2_0.base;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.core.jobserver.JobserverJobInput;
import com.knime.bigdata.spark.core.jobserver.JobserverJobOutput;
import com.knime.bigdata.spark.core.jobserver.TypesafeConfigSerializationUtils;
import com.knime.bigdata.spark.jobserver.server.KNIMESparkJob;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SimpleSparkJob;
import com.knime.bigdata.spark2_0.api.SparkJob;
import com.knime.bigdata.spark2_0.api.SparkJobWithFiles;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import spark.jobserver.api.JobEnvironment;

/**
 * handles translation of Scala interface to Java, wraps generic config with JobConfig
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class JobserverSparkJob extends KNIMESparkJob implements NamedObjects {

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
            List<File> inputFiles = validateInputFiles(jsInput);

            Object sparkJob = getClass().getClassLoader().loadClass(jsInput.getSparkJobClass()).newInstance();

            // FIXME this is quite probably broken when multiple jobs run at the same time. in
            // this case we may get log messages from other jobs within the same context.
            appender = new InterceptingAppender(jsInput.getLog4jLogLevel());
            Logger.getRootLogger().addAppender(appender);

            if (sparkJob instanceof SparkJob) {
                toReturn = JobserverJobOutput.success(((SparkJob) sparkJob).runJob(sparkContext, input, this));
            } else if (sparkJob instanceof SparkJobWithFiles){
                toReturn = JobserverJobOutput.success(((SparkJobWithFiles) sparkJob).runJob(sparkContext, input, inputFiles, this));
            } else {
                ((SimpleSparkJob) sparkJob).runJob(sparkContext, input, this);
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

    private List<File> validateInputFiles(final JobserverJobInput jsInput) throws KNIMESparkException {
        List<File> inputFiles = new LinkedList<>();

        for (String pathToFile : jsInput.getFiles()) {
            File inputFile = new File(pathToFile);
            if (inputFile.canRead()) {
                inputFiles.add(inputFile);
            } else {
                throw new KNIMESparkException("Cannot read input file on jobserver: " + pathToFile);
            }
        }

        return inputFiles;
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

    @Deprecated
    @Override
    public JavaRDD<Row> getJavaRdd(final String key) {
        return getDataFrame(key).javaRDD();
    }
}

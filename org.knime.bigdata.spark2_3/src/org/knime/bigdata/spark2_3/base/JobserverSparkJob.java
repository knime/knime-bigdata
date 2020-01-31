package org.knime.bigdata.spark2_3.base;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.context.namedobjects.SparkDataObjectStatistic;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.JobserverJobInput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.TypesafeConfigSerializationUtils;
import org.knime.bigdata.spark2_3.api.NamedObjects;
import org.knime.bigdata.spark2_3.api.SimpleSparkJob;
import org.knime.bigdata.spark2_3.api.SparkJob;
import org.knime.bigdata.spark2_3.api.SparkJobWithFiles;

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

    private static final String CANNOT_READ_JOBSERVER_FILE = "Cannot read input file on jobserver: ";

    /** Empty deserialization constructor */
    public JobserverSparkJob() {
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public String runJob(final SparkContext sparkContext, final JobEnvironment runtime, final Config config) {
        WrapperJobOutput toReturn;

        try {
            final JobserverJobInput jsInput = TypesafeConfigSerializationUtils.deserializeJobserverJobInput(config);

            final JobInput input = jsInput.getSparkJobInput();

            NamedObjectsImpl.ensureNamedInputObjectsExist(input);
            NamedObjectsImpl.ensureNamedOutputObjectsDoNotExist(input);
            List<File> inputFiles = validateInputFiles(runtime, jsInput);

            Object sparkJob = getClass().getClassLoader().loadClass(jsInput.getSparkJobClass()).newInstance();

            if (sparkJob instanceof SparkJob) {
                toReturn = WrapperJobOutput
                    .success(((SparkJob)sparkJob).runJob(sparkContext, input, NamedObjectsImpl.SINGLETON_INSTANCE));
            } else if (sparkJob instanceof SparkJobWithFiles) {
                toReturn = WrapperJobOutput.success(((SparkJobWithFiles)sparkJob).runJob(sparkContext, input,
                    inputFiles, NamedObjectsImpl.SINGLETON_INSTANCE));
            } else {
                ((SimpleSparkJob)sparkJob).runJob(sparkContext, input, NamedObjectsImpl.SINGLETON_INSTANCE);
                toReturn = WrapperJobOutput.success();
            }

            addDataFrameNumPartitions(input.getNamedOutputObjects(), toReturn, NamedObjectsImpl.SINGLETON_INSTANCE);

        } catch (KNIMESparkException e) {
            toReturn = WrapperJobOutput.failure(e);
        } catch (Throwable t) {
            toReturn = WrapperJobOutput.failure(new KNIMESparkException(t));
        }

        return TypesafeConfigSerializationUtils.serializeToTypesafeConfig(toReturn).root()
            .render(ConfigRenderOptions.concise());
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

    private static List<File> validateInputFiles(final JobEnvironment runtime, final JobserverJobInput jsInput)
        throws KNIMESparkException {
        final List<File> inputFiles = new LinkedList<>();

        if (runtime instanceof DataFileCache) {
            final DataFileCache fileCache = (DataFileCache)runtime;

            for (final String pathToFile : jsInput.getJobServerFiles()) {
                try {
                    final File inputFile = fileCache.getDataFile(pathToFile);

                    if (inputFile.canRead()) {
                        inputFiles.add(inputFile);
                    } else {
                        throw new KNIMESparkException(CANNOT_READ_JOBSERVER_FILE + pathToFile);
                    }

                } catch (final IOException e) {
                    throw new KNIMESparkException(CANNOT_READ_JOBSERVER_FILE + pathToFile, e);
                }
            }

        } else {
            for (final String pathToFile : jsInput.getJobServerFiles()) {
                final File currentFile = new File(pathToFile);
                if (currentFile.canRead()) {
                    inputFiles.add(currentFile);
                } else {
                    throw new KNIMESparkException(CANNOT_READ_JOBSERVER_FILE + pathToFile);
                }
            }
        }

        return inputFiles;
    }
}

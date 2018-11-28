package org.knime.bigdata.spark2_4.base;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.JobserverJobInput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.JobserverJobOutput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.TypesafeConfigSerializationUtils;
import org.knime.bigdata.spark2_4.api.SimpleSparkJob;
import org.knime.bigdata.spark2_4.api.SparkJob;
import org.knime.bigdata.spark2_4.api.SparkJobWithFiles;

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

    /** Empty deserialization constructor */
    public JobserverSparkJob() {
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public String runJob(final SparkContext sparkContext, final JobEnvironment runtime, final Config config) {
        JobserverJobOutput toReturn;

        try {
            final JobserverJobInput jsInput = JobserverJobInput.createFromMap(TypesafeConfigSerializationUtils
                .deserializeFromTypesafeConfig(config, this.getClass().getClassLoader()));

            final JobInput input = jsInput.getSparkJobInput();

            NamedObjectsImpl.ensureNamedInputObjectsExist(input);
            NamedObjectsImpl.ensureNamedOutputObjectsDoNotExist(input);
            List<File> inputFiles = validateInputFiles(runtime, jsInput);

            Object sparkJob = getClass().getClassLoader().loadClass(jsInput.getSparkJobClass()).newInstance();

            if (sparkJob instanceof SparkJob) {
                toReturn = JobserverJobOutput
                    .success(((SparkJob)sparkJob).runJob(sparkContext, input, NamedObjectsImpl.SINGLETON_INSTANCE));
            } else if (sparkJob instanceof SparkJobWithFiles) {
                toReturn = JobserverJobOutput.success(((SparkJobWithFiles)sparkJob).runJob(sparkContext, input,
                    inputFiles, NamedObjectsImpl.SINGLETON_INSTANCE));
            } else {
                ((SimpleSparkJob)sparkJob).runJob(sparkContext, input, NamedObjectsImpl.SINGLETON_INSTANCE);
                toReturn = JobserverJobOutput.success();
            }
        } catch (KNIMESparkException e) {
            toReturn = JobserverJobOutput.failure(e);
        } catch (Throwable t) {
            toReturn = JobserverJobOutput.failure(new KNIMESparkException(t));
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
}

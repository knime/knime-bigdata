package org.knime.bigdata.spark1_2.base;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.context.namedobjects.SparkDataObjectStatistic;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.JobserverJobInput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.TypesafeConfigSerializationUtils;
import org.knime.bigdata.spark1_2.api.NamedObjects;
import org.knime.bigdata.spark1_2.api.SimpleSparkJob;
import org.knime.bigdata.spark1_2.api.SparkJob;
import org.knime.bigdata.spark1_2.api.SparkJobWithFiles;

import com.knime.bigdata.spark.jobserver.server.KnimeSparkJobWithNamedRDD;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import spark.jobserver.SparkJobValid$;
import spark.jobserver.SparkJobValidation;

/**
 * handles translation of Scala interface to Java, wraps generic config with JobConfig
 *
 * @author dwk
 * @author Bjoern Lohrmann, KNIME.com
 *
 */
@SparkClass
public class JobserverSparkJob extends KnimeSparkJobWithNamedRDD implements NamedObjects {

    /**
     * Empty default constructor.
     */
    public JobserverSparkJob() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Object runJob(final Object sparkContext, final Config config) {

        WrapperJobOutput toReturn;

        try {
            final JobserverJobInput jsInput = TypesafeConfigSerializationUtils.deserializeJobserverJobInput(config);

            final JobInput input = jsInput.getSparkJobInput();

            ensureNamedInputObjectsExist(input);
            ensureNamedOutputObjectsDoNotExist(input);
            List<File> inputFiles = validateInputFiles(jsInput);

            Object sparkJob = getClass().getClassLoader().loadClass(jsInput.getSparkJobClass()).newInstance();

            if (sparkJob instanceof SparkJob) {
                toReturn = WrapperJobOutput
                    .success(((SparkJob)sparkJob).runJob((SparkContext)sparkContext, input, this));
            } else if (sparkJob instanceof SparkJobWithFiles) {
                toReturn = WrapperJobOutput.success(((SparkJobWithFiles)sparkJob).runJob((SparkContext)sparkContext, input,
                    inputFiles, this));
            } else {
                ((SimpleSparkJob)sparkJob).runJob((SparkContext)sparkContext, input, this);
                toReturn = WrapperJobOutput.success();
            }

            addDataFrameNumPartitions(input.getNamedOutputObjects(), toReturn, this);

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

                if (namedObjects.validateNamedObject(key)) {
                    final JavaRDD<Row> rdd = namedObjects.getJavaRdd(key);
                    final NamedObjectStatistics stat = new SparkDataObjectStatistic(rdd.partitions().size());
                    jobOutput.setNamedObjectStatistic(key, stat);
                }
            }
        }
    }

    private static List<File> validateInputFiles(final JobserverJobInput jsInput) throws KNIMESparkException {
        List<File> inputFiles = new LinkedList<File>();

        for (Path pathToFile : jsInput.getFiles()) {
            if (Files.isReadable(pathToFile)) {
                inputFiles.add(pathToFile.toFile());
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

    @Override
    public final SparkJobValidation validate(final Object aSparkContext, final Config config) {
        // in scala this is a case object and this is the way these are referenced from Java.
        return SparkJobValid$.MODULE$;
    }
}

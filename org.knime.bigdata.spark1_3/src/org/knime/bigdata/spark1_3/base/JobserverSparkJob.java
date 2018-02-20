package org.knime.bigdata.spark1_3.base;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.JobserverJobInput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.JobserverJobOutput;
import org.knime.bigdata.spark.core.sparkjobserver.jobapi.TypesafeConfigSerializationUtils;
import org.knime.bigdata.spark1_3.api.NamedObjects;
import org.knime.bigdata.spark1_3.api.SimpleSparkJob;
import org.knime.bigdata.spark1_3.api.SparkJob;
import org.knime.bigdata.spark1_3.api.SparkJobWithFiles;

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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Object runJob(final Object sparkContext, final Config config) {

        JobserverJobOutput toReturn;

        try {
            final JobserverJobInput jsInput = JobserverJobInput.createFromMap(TypesafeConfigSerializationUtils
                .deserializeFromTypesafeConfig(config, this.getClass().getClassLoader()));

            final JobInput input = jsInput.getSparkJobInput();

            ensureNamedInputObjectsExist(input);
            ensureNamedOutputObjectsDoNotExist(input);
            List<File> inputFiles = validateInputFiles(jsInput);

            Object sparkJob = getClass().getClassLoader().loadClass(jsInput.getSparkJobClass()).newInstance();

            if (sparkJob instanceof SparkJob) {
                toReturn = JobserverJobOutput.success(((SparkJob)sparkJob).runJob((SparkContext)sparkContext, input, this));
            } else if (sparkJob instanceof SparkJobWithFiles){
                toReturn = JobserverJobOutput.success(((SparkJobWithFiles)sparkJob).runJob((SparkContext)sparkContext, input, inputFiles, this));
            } else {
                ((SimpleSparkJob)sparkJob).runJob((SparkContext)sparkContext, input, this);
                toReturn = JobserverJobOutput.success();
            }
        } catch (KNIMESparkException e) {
            toReturn = JobserverJobOutput.failure(e);
        } catch (Throwable t) {
            toReturn = JobserverJobOutput.failure(new KNIMESparkException("Failed to execute Spark job: " + t.getMessage(), t));
        }

        return TypesafeConfigSerializationUtils.serializeToTypesafeConfig(toReturn.getInternalMap()).root()
            .render(ConfigRenderOptions.concise());
    }

    private List<File> validateInputFiles(final JobserverJobInput jsInput) throws KNIMESparkException {
        List<File> inputFiles = new LinkedList<File>();

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

    @Override
    public final SparkJobValidation validate(final Object aSparkContext, final Config config) {
        // in scala this is a case object and this is the way these are referenced from Java.
        return SparkJobValid$.MODULE$;
    }
}

/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark3_0.base;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.context.namedobjects.SparkDataObjectStatistic;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksJobInput;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksJobSerializationUtils;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksSparkSideStagingArea;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark3_0.api.NamedObjects;
import org.knime.bigdata.spark3_0.api.SimpleSparkJob;
import org.knime.bigdata.spark3_0.api.SparkJob;
import org.knime.bigdata.spark3_0.api.SparkJobWithFiles;

/**
 * Entry point for Spark jobs on Databricks.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class DatabricksSparkJob {
    private static final Logger LOGGER = Logger.getLogger(DatabricksSparkJob.class.getName());

    private final String m_jobId;

    /**
     * Public constructor invoked by {@link DatabricksSparkREPLContext}.
     *
     * @param jobId unique job identifier
     */
    public DatabricksSparkJob(final String jobId) {
        m_jobId = jobId;
    }

    /**
     * Create a new context instance.
     *
     * @param sc {@link SparkContext} to use
     * @param stagingArea URI or path of staging area
     * @param stagingAreaIsPath <code>true</code> if staging area is a path on default Hadoop FS
     * @return new context instance
     * @throws Exception
     */
    public static DatabricksSparkREPLContext createContext(final SparkContext sc, final String stagingArea, final boolean stagingAreaIsPath) throws Exception {
        return new DatabricksSparkREPLContext(sc, stagingArea, stagingAreaIsPath);
    }

    /**
     * Run the spark job in given context.
     *
     * @param sc spark context to use
     * @param stagingArea staging area to use
     * @throws Exception
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void call(final SparkContext sc, final DatabricksSparkSideStagingArea stagingArea) throws Exception {
        WrapperJobOutput toReturn;

        try {
            final DatabricksJobInput wrappedJobInput =
                DatabricksJobSerializationUtils.deserializeJobInputFromStagingFile(m_jobId, getClass().getClassLoader(), stagingArea);
            final JobInput jobInput = wrappedJobInput.getSparkJobInput();

            NamedObjectsImpl.ensureNamedInputObjectsExist(jobInput);
            NamedObjectsImpl.ensureNamedOutputObjectsDoNotExist(jobInput);

            Object sparkJob = getClass().getClassLoader().loadClass(wrappedJobInput.getSparkJobClass()).newInstance();
            LOGGER.info("Running Spark job: " + wrappedJobInput.getSparkJobClass());

            if (sparkJob instanceof SparkJob) {
                toReturn = WrapperJobOutput
                    .success(((SparkJob)sparkJob).runJob(sc, jobInput, NamedObjectsImpl.SINGLETON_INSTANCE));
            } else if (sparkJob instanceof SparkJobWithFiles) {
                final List<File> inputFiles =
                    jobInput.getFiles().stream().map(p -> p.toFile()).collect(Collectors.toList());
                toReturn = WrapperJobOutput.success(((SparkJobWithFiles)sparkJob).runJob(sc, jobInput, inputFiles,
                    NamedObjectsImpl.SINGLETON_INSTANCE));
            } else {
                ((SimpleSparkJob)sparkJob).runJob(sc, jobInput, NamedObjectsImpl.SINGLETON_INSTANCE);
                toReturn = WrapperJobOutput.success();
            }

            addDataFrameNumPartitions(jobInput.getNamedOutputObjects(), toReturn, NamedObjectsImpl.SINGLETON_INSTANCE);

        } catch (KNIMESparkException e) {
            toReturn = WrapperJobOutput.failure(e);
        } catch (Throwable t) {
            toReturn = WrapperJobOutput.failure(new KNIMESparkException(t));
        }

        try {
            // this call to StagingAreaUtil.toSerializedMap() may involve I/O to HDFS/S3/... and can thus fail
            DatabricksJobSerializationUtils.serializeJobOutputToStagingFile(m_jobId, toReturn, stagingArea);
        } catch (Throwable e) {
            // this call to StagingAreaUtil.toSerializedMap () will NOT fail, because Throwables receive
            // special treatment
            final WrapperJobOutput failure = WrapperJobOutput.failure(new KNIMESparkException(e));
            DatabricksJobSerializationUtils.serializeJobOutputToStagingFile(m_jobId, failure, stagingArea);
        }
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

}

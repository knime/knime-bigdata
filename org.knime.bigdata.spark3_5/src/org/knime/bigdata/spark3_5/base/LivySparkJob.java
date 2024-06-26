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
 *
 * History
 *   Created on Aug 17, 2017 by bjoern
 */
package org.knime.bigdata.spark3_5.base;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.context.namedobjects.SparkDataObjectStatistic;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobInput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobSerializationUtils;
import org.knime.bigdata.spark.core.livy.jobapi.SparkSideStagingArea;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SimpleSparkJob;
import org.knime.bigdata.spark3_5.api.SparkConfigUtil;
import org.knime.bigdata.spark3_5.api.SparkJob;
import org.knime.bigdata.spark3_5.api.SparkJobWithFiles;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SparkClass
public class LivySparkJob implements Job<WrapperJobOutput> {

    private static final long serialVersionUID = 1L;

    private final LivyJobInput m_livyInput;

    /**
     * Public constructor invoked by Livy.
     *
     * @param input The deserialized job input.
     */
    public LivySparkJob(final LivyJobInput input) {
        m_livyInput = input;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public WrapperJobOutput call(final JobContext ctx) throws Exception {
        WrapperJobOutput toReturn;

        LivyJobSerializationUtils.postKryoDeserialize(m_livyInput, getClass().getClassLoader(),
            SparkSideStagingArea.SINGLETON_INSTANCE);

        try {

            final JobInput jobInput = m_livyInput.getSparkJobInput();

            NamedObjectsImpl.ensureNamedInputObjectsExist(jobInput);
            NamedObjectsImpl.ensureNamedOutputObjectsDoNotExist(jobInput);

            Object sparkJob = getClass().getClassLoader().loadClass(m_livyInput.getSparkJobClass()).newInstance();

            final SparkContext sc = ctx.sc().sc();

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

            if (!SparkConfigUtil.adaptiveExecutionEnabled(sc)) {
                addDataFrameNumPartitions(jobInput.getNamedOutputObjects(), toReturn, NamedObjectsImpl.SINGLETON_INSTANCE);
            }

        } catch (KNIMESparkException e) {
            toReturn = WrapperJobOutput.failure(e);
        } catch (Throwable t) {
            toReturn = WrapperJobOutput.failure(new KNIMESparkException(t));
        }

        try {
            // this call to StagingAreaUtil.toSerializedMap() may involve I/O to HDFS/S3/... and can thus fail
            return LivyJobSerializationUtils.preKryoSerialize(toReturn, SparkSideStagingArea.SINGLETON_INSTANCE,
                new WrapperJobOutput());
        } catch (Throwable e) {
            // this call to StagingAreaUtil.toSerializedMap () will NOT fail, because Throwables receive
            // special treatment
            final WrapperJobOutput failure = WrapperJobOutput.failure(new KNIMESparkException(e));
            return LivyJobSerializationUtils.preKryoSerialize(failure, SparkSideStagingArea.SINGLETON_INSTANCE,
                new WrapperJobOutput());
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

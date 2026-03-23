/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark3_5.jobs.util;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.Utils;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.util.repartition.RepartitionJobInput;
import org.knime.bigdata.spark.node.util.repartition.RepartitionJobInput.CalculationMode;
import org.knime.bigdata.spark.node.util.repartition.RepartitionJobOutput;
import org.knime.bigdata.spark3_5.api.NamedObjects;
import org.knime.bigdata.spark3_5.api.SparkJob;

/**
 * Repartition / coalesce data frames.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class RepartitionJob implements SparkJob<RepartitionJobInput, RepartitionJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(RepartitionJob.class.getName());

    @Override
    public RepartitionJobOutput runJob(final SparkContext sparkContext, final RepartitionJobInput input, final NamedObjects namedObjects)
            throws KNIMESparkException {

        final Dataset<Row> inDataFrame = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        int executors = -1;
        final int currentNumberOfPartitions = inDataFrame.rdd().getNumPartitions();
        final int newNumberOfPartitions;
        final Dataset<Row> outDataFrame;

        switch (input.getCalculationMode()) {
            case FIXED_VALUE:
                newNumberOfPartitions = input.getFixedValue();
                break;

            case MULTIPLY_PART_COUNT:
                newNumberOfPartitions = Math.max(1, (int)(currentNumberOfPartitions * input.getFactor()));
                break;

            case DIVIDE_PART_COUNT:
                newNumberOfPartitions = Math.max(1, (int)(currentNumberOfPartitions / input.getFactor()));
                break;

            case MULTIPLY_EXECUTOR_CORES:
                final String master = sparkContext.conf().get("spark.master");

                if (master.matches("local\\[\\d+\\]")) {
                    final int cores = Integer.parseInt(master.replaceAll("local\\[(\\d+)\\]", "$1"));
                    newNumberOfPartitions = Math.max(1, (int)(input.getFactor() * cores));
                } else if (master.startsWith("local")){
                    throw new KNIMESparkException("Unable to read thread count from local spark string: " + master);
                } else {
                    final int cores = sparkContext.conf().getInt("spark.executor.cores", 1);
                    executors = sparkContext.getExecutorIds().size();
                    newNumberOfPartitions = Math.max(1, (int)(input.getFactor() * executors * cores));
                }
                break;

            default:
              throw new KNIMESparkException("Unknown new number of partitions calculation method.");
        }

        if (input.useCoalesce() && newNumberOfPartitions < currentNumberOfPartitions) {
            LOGGER.info(String.format(
                "Repartitioning from %d to %d partitions using coalesce.", currentNumberOfPartitions, newNumberOfPartitions));
            outDataFrame = inDataFrame.coalesce(newNumberOfPartitions);
        } else {
            LOGGER.info(String.format(
                "Repartitioning from %d to %d partitions using repartition.", currentNumberOfPartitions, newNumberOfPartitions));
            outDataFrame = inDataFrame.repartition(newNumberOfPartitions);
        }

        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), outDataFrame);

        if (input.getCalculationMode() == CalculationMode.MULTIPLY_EXECUTOR_CORES
                && Utils.isDynamicAllocationEnabled(sparkContext.conf())) {
            return RepartitionJobOutput.dynamicAllocation(executors);
        } else {
            return RepartitionJobOutput.success();
        }
    }
}

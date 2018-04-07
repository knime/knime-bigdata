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
 *
 * History
 *   Created on Jan 29, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark2_2.jobs.mllib.freqitemset;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.freqitemset.FrequentItemSetJobInput;
import org.knime.bigdata.spark.node.mllib.freqitemset.FrequentItemSetJobOutput;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SparkJob;

/**
 * Find frequent item sets using FP-Growth in Spark.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class FrequentItemSetJob implements SparkJob<FrequentItemSetJobInput, FrequentItemSetJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(FrequentItemSetJob.class.getName());

    @Override
    public FrequentItemSetJobOutput runJob(final SparkContext sparkContext, final FrequentItemSetJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        LOGGER.info("Running frequent items job...");

        final Dataset<Row> inputRows = namedObjects.getDataFrame(input.getItemsInputObject());
        final FPGrowth fpg = new FPGrowth()
                .setItemsCol(input.getItemColumn())
                .setMinSupport(input.getMinSupport());
        if (input.hasNumPartitions()) {
            LOGGER.warn("Using custom number of partitions: " + input.getNumPartitions());
            fpg.setNumPartitions(input.getNumPartitions());
        }

        final FPGrowthModel model = fpg.fit(inputRows);
        namedObjects.addDataFrame(input.getFreqItemsOutputObject(), model.freqItemsets());

        LOGGER.info("Frequent items job done.");

        return new FrequentItemSetJobOutput(FrequentItemSetModel.fromJobInput(model.uid(), input));
    }
}

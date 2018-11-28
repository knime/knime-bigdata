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
package org.knime.bigdata.spark2_4.jobs.mllib.freqitemset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.freqitemset.FrequentItemSetJobInput;
import org.knime.bigdata.spark2_4.api.NamedObjects;
import org.knime.bigdata.spark2_4.api.SimpleSparkJob;


/**
 * Find frequent item sets using FP-Growth in Spark.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class FrequentItemSetJob implements SimpleSparkJob<FrequentItemSetJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(FrequentItemSetJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final FrequentItemSetJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        LOGGER.info("Running frequent items job...");

        final Dataset<Row> inputRows = namedObjects.getDataFrame(input.getItemsInputObject())
                .na().drop(new String[] { input.getItemColumn() });
        final FPGrowth fpg = new FPGrowth().setItemsCol(input.getItemColumn()).setMinSupport(input.getMinSupport());

        if (input.hasNumPartitions()) {
            LOGGER.warn("Using custom number of partitions: " + input.getNumPartitions());
            fpg.setNumPartitions(input.getNumPartitions());
        }

        final FPGrowthModel model = fpg.fit(inputRows);
        final Dataset<Row> result = model.freqItemsets()
            .select(col("items").as("ItemSet"), size(col("items")).as("ItemSetSize"), col("freq").as("ItemSetSupport"));
        namedObjects.addDataFrame(input.getFreqItemsOutputObject(), result);

        LOGGER.info("Frequent items job done.");
    }
}

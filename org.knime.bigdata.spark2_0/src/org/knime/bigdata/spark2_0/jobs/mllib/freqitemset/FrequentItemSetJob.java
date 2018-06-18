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
package org.knime.bigdata.spark2_0.jobs.mllib.freqitemset;

import static org.apache.spark.sql.functions.col;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.freqitemset.FrequentItemSetJobInput;
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.RowBuilder;
import org.knime.bigdata.spark2_0.api.SimpleSparkJob;

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

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

        // extract items field into RDD
        final Dataset<Row> inputRows = namedObjects.getDataFrame(input.getItemsInputObject());
        final StructField itemsField = inputRows.schema().apply(input.getItemColumn());
        final JavaRDD<List<Object>> items = inputRows
            .select(col(input.getItemColumn())).na().drop("any")
            .javaRDD().map(new Function<Row, List<Object>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public List<Object> call(final Row row) throws Exception {
                    return row.getList(0);
                }
            });

        // do it
        final FPGrowth fpg = new FPGrowth().setMinSupport(input.getMinSupport());
        if (input.hasNumPartitions()) {
            LOGGER.warn("Using custom number of partitions: " + input.getNumPartitions());
            fpg.setNumPartitions(input.getNumPartitions());
        }
        final FPGrowthModel<Object> model = fpg.run(items);

        // convert frequent items
        final JavaRDD<Row> freqItems = model
            .freqItemsets()
            .toJavaRDD().map(new Function<FPGrowth.FreqItemset<Object>, Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Row call(final FPGrowth.FreqItemset<Object> itemset){
                    RowBuilder rb = RowBuilder.emptyRow();
                    rb.add(itemset.javaItems().toArray());
                    rb.add(itemset.javaItems().size());
                    rb.add(itemset.freq());
                    return rb.build();
                }});
        namedObjects.addDataFrame(input.getFreqItemsOutputObject(),
            spark.createDataFrame(freqItems, freqItemsSchema(itemsField)));

        LOGGER.info("Frequent items job done.");
    }

    private static StructType freqItemsSchema(final StructField itemsField) {
        return new StructType(new StructField[]{
            DataTypes.createStructField("ItemSet", itemsField.dataType(), false),
            DataTypes.createStructField("ItemSetSize", DataTypes.IntegerType, false),
            DataTypes.createStructField("ItemSetSupport", DataTypes.LongType, false) });
    }
}

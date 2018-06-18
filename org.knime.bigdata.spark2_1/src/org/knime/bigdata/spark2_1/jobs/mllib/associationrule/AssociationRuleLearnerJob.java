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
package org.knime.bigdata.spark2_1.jobs.mllib.associationrule;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.AssociationRules.Rule;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleLearnerJobInput;
import org.knime.bigdata.spark2_1.api.NamedObjects;
import org.knime.bigdata.spark2_1.api.RowBuilder;
import org.knime.bigdata.spark2_1.api.SimpleSparkJob;

/**
 * Implements a association rules learner using frequent pattern mining in spark.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class AssociationRuleLearnerJob implements SimpleSparkJob<AssociationRuleLearnerJobInput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(AssociationRuleLearnerJob.class.getName());

    @Override
    public void runJob(final SparkContext sparkContext, final AssociationRuleLearnerJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        LOGGER.info("Generating association rules...");

        @SuppressWarnings("resource")
        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final Dataset<Row> freqItemsDataset = namedObjects.getDataFrame(input.getFreqItemSetsInputObject())
                .select(col("ItemSet"), col("ItemSetSupport")).na().drop("any");
        final DataType itemsFieldDataType = freqItemsDataset.schema().apply("ItemSet").dataType();

        // convert frequent items to rdd
        final JavaRDD<FPGrowth.FreqItemset<Object>> freqItemsets = freqItemsDataset
            .toJavaRDD().map(new Function<Row, FPGrowth.FreqItemset<Object>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public FPGrowth.FreqItemset<Object> call(final Row row) {
                    return new FPGrowth.FreqItemset<>(row.getList(0).toArray(), row.getLong(1));
                }});

        // do it
        final AssociationRules ruleModel = new AssociationRules(input.getMinConfidence());
        final JavaRDD<Rule<Object>> assRulesRdd = ruleModel.run(freqItemsets);

        // convert association rules to dataset
        final JavaRDD<Row> associationRulesRDD = assRulesRdd
            .map(new Function<AssociationRules.Rule<Object>, Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Row call(final AssociationRules.Rule<Object> rule){
                    RowBuilder rb = RowBuilder.emptyRow();
                    rb.add(rule.javaConsequent().toArray());
                    rb.add(rule.javaAntecedent().toArray());
                    rb.add(rule.confidence());
                    rb.add(rule.confidence() * 100);
                    return rb.build();
                }});
        // explode consequent
        final Dataset<Row> associationRules = spark
            .createDataFrame(associationRulesRDD, associationRulesSchema(itemsFieldDataType))
            .select(explode(col("Consequent")).as("Consequent"), col("Antecedent"), col("RuleConfidence"), col("RuleConfidence%"));
        namedObjects.addDataFrame(input.getAssociationRulesOutputObject(), associationRules);

        LOGGER.info("Association rules learner done.");
    }

    private static StructType associationRulesSchema(final DataType itemsFieldDataType) {
        return new StructType(new StructField[]{
            DataTypes.createStructField("Consequent", itemsFieldDataType, false),
            DataTypes.createStructField("Antecedent", itemsFieldDataType, false),
            DataTypes.createStructField("RuleConfidence", DataTypes.DoubleType, false),
            DataTypes.createStructField("RuleConfidence%", DataTypes.DoubleType, false) });
    }
}

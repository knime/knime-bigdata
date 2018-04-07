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
package org.knime.bigdata.spark2_0.jobs.mllib.associationrule;

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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleLearnerJobInput;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleLearnerJobOutput;
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.RowBuilder;
import org.knime.bigdata.spark2_0.api.SparkJob;
import org.knime.bigdata.spark2_0.jobs.mllib.freqitemset.FrequentItemSetModel;

/**
 * Implements a association rules learner using frequent pattern mining in spark.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class AssociationRuleLearnerJob implements SparkJob<AssociationRuleLearnerJobInput, AssociationRuleLearnerJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(AssociationRuleLearnerJob.class.getName());

    @Override
    public AssociationRuleLearnerJobOutput runJob(final SparkContext sparkContext, final AssociationRuleLearnerJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        LOGGER.info("Generating association rules...");

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        final FrequentItemSetModel freqItemsModel = (FrequentItemSetModel)input.getFreqItemsModel();
        final Dataset<Row> freqItemsDataset = namedObjects.getDataFrame(freqItemsModel.getFrequentItemsObjectName());
        final StructField itemsField = freqItemsDataset.schema().apply("items");

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
        final JavaRDD<Row> associationRules = assRulesRdd
            .map(new Function<AssociationRules.Rule<Object>, Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Row call(final AssociationRules.Rule<Object> rule){
                    RowBuilder rb = RowBuilder.emptyRow();
                    rb.add(rule.javaAntecedent().toArray());
                    rb.add(rule.javaConsequent().toArray());
                    rb.add(rule.confidence());
                    return rb.build();
                }});
        namedObjects.addDataFrame(input.getAssociationRulesOutputObject(),
            spark.createDataFrame(associationRules, associationRulesSchema(itemsField)));

        LOGGER.info("Association rules learner done.");

        return new AssociationRuleLearnerJobOutput(
            new AssociationRuleModel(input.getAssociationRulesOutputObject(), input.getMinConfidence(), "antecedent", "consequent"));
    }

    private StructType associationRulesSchema(final StructField itemsField) {
        return new StructType(new StructField[]{
            DataTypes.createStructField("antecedent", itemsField.dataType(), false),
            DataTypes.createStructField("consequent", itemsField.dataType(), false),
            DataTypes.createStructField("confidence", DataTypes.DoubleType, false) });
    }
}

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

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleApplyJobInput;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleApplyJobOutput;
import org.knime.bigdata.spark2_0.api.NamedObjects;
import org.knime.bigdata.spark2_0.api.RowBuilder;
import org.knime.bigdata.spark2_0.api.SparkJob;

import scala.Tuple2;

/**
 * Implements a association rules apply spark job.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class AssociationRuleApplyJob implements SparkJob<AssociationRuleApplyJobInput, AssociationRuleApplyJobOutput> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(AssociationRuleApplyJob.class.getName());

    @Override
    public AssociationRuleApplyJobOutput runJob(final SparkContext sparkContext, final AssociationRuleApplyJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        LOGGER.info("Generating association rules...");

        final SparkSession spark = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
        @SuppressWarnings("resource") // don't close JavaSparkContext
        final JavaSparkContext javaSparkContex = new JavaSparkContext(sparkContext);
        final AssociationRuleModel ruleModel = (AssociationRuleModel) input.getRuleModel();
        final Dataset<Row> rulesDataset = namedObjects.getDataFrame(ruleModel.getAssociationRulesObjectName());
        final Dataset<Row> itemsDataset = namedObjects.getDataFrame(input.getItemsInputObject());

        // load and broadcast rules
        final JavaRDD<Tuple2<List<Object>, List<Object>>> rulesRdd = rulesDataset
                .select(ruleModel.getAntecedentColumn(), ruleModel.getConsequentColumn())
                .javaRDD().map(new Function<Row, Tuple2<List<Object>, List<Object>>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<List<Object>, List<Object>> call(final Row row) throws Exception {
                        return new Tuple2<>(row.getList(0), row.getList(1));
                    }
                });
        final List<Tuple2<List<Object>, List<Object>>> rules;
        if (input.hasRuleLimit()) {
            rules = rulesRdd.take(input.getRuleLimit());
        } else {
            rules = rulesRdd.collect();
        }
        final Broadcast<List<Tuple2<List<Object>, List<Object>>>> brRules = javaSparkContex.broadcast(rules);

        // apply rules
        final StructField itemsField = itemsDataset.schema().apply(input.getItemColumn());
        final int itemsFieldIdx = itemsDataset.schema().fieldIndex(input.getItemColumn());
        final Dataset<Row> result = spark.createDataFrame(
            itemsDataset.toJavaRDD().map(new RulesApplyFunction(itemsFieldIdx, brRules)),
            itemsDataset.schema().add(input.getOutputColumn(), itemsField.dataType(), false));
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), result);

        LOGGER.info("Association rules learner done.");

        return new AssociationRuleApplyJobOutput(rules.size());
    }

    /**
     * Applies broadcasted association rules to item collections in a map step.
     *
     * A rule matches if all antecedent items are part of the given item set. The final predictions are a distinct set
     * with items that not already part of the input item set.
     *
     * Implementation was extracted from ml-FPGrowth transform implementation in Spark 2.2.
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public class RulesApplyFunction implements Function<Row, Row> {
        private static final long serialVersionUID = 1L;
        private final int m_inputColIndex;
        private final Broadcast<List<Tuple2<List<Object>, List<Object>>>> m_brRules;

        /**
         * Default constructor.
         *
         * @param inputColIndex collection column index in input dataset
         * @param brRules rule tuples of antecedent and consequent item lists
         */
        public RulesApplyFunction(final int inputColIndex, final Broadcast<List<Tuple2<List<Object>, List<Object>>>> brRules) {
            m_inputColIndex = inputColIndex;
            m_brRules = brRules;
        }

        @Override
        public Row call(final Row inputRow) throws Exception {
            final RowBuilder rb = RowBuilder.fromRow(inputRow);
            final List<Object> items = inputRow.getList(m_inputColIndex);

            if (items != null) {
                final List<Object> result = new ArrayList<>();
                for (Tuple2<List<Object>, List<Object>> rule : m_brRules.getValue()) {
                    applyRule(items, rule, result);
                }
                rb.add(result.toArray());

            } else {
                rb.add(new Object[0]);
            }

            return rb.build();
        }

        /**
         * If input items contains all antecedent items of the rule, add all new consequent items to the result set.
         *
         * @param items input item set
         * @param rule tuple with antecedent and consequent item lists
         * @param result predicted items set
         */
        private void applyRule(final List<Object> items, final Tuple2<List<Object>, List<Object>> rule,
            final List<Object> result) {

            if (items.containsAll(rule._1())) {
                for (Object o : rule._2()) {
                    if (!items.contains(o) && !result.contains(o)) { // add new items
                        result.add(o);
                    }
                }
            }
        }
    }
}

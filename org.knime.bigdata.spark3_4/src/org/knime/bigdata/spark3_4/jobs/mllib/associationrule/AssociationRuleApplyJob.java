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
package org.knime.bigdata.spark3_4.jobs.mllib.associationrule;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleApplyJobInput;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleApplyJobOutput;
import org.knime.bigdata.spark3_4.api.NamedObjects;
import org.knime.bigdata.spark3_4.api.RowBuilder;
import org.knime.bigdata.spark3_4.api.SparkJob;

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
    public AssociationRuleApplyJobOutput runJob(final SparkContext sparkContext,
        final AssociationRuleApplyJobInput input, final NamedObjects namedObjects) throws KNIMESparkException {

        LOGGER.info("Predicting items using association rules...");

        @SuppressWarnings("resource") // we don't close the context here
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        final Dataset<Row> itemsDataset = namedObjects.getDataFrame(input.getItemsInputObject())
            .na().drop(new String[] { input.getItemColumn() });

        // load and broadcast rules
        final JavaRDD<Tuple2<List<Object>, Object>> rulesDataset = namedObjects.getDataFrame(input.getRulesInputObject())
            .select(input.getAntecedentColumn(), input.getConsequentColumn())
            .na().drop("any").javaRDD().map(new Function<Row, Tuple2<List<Object>, Object>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<List<Object>, Object> call(final Row row) throws Exception {
                    return Tuple2.apply(row.getList(0), row.get(1));
                }
            });
        final List<Tuple2<List<Object>, Object>> rules;
        if (input.hasRuleLimit()) {
            rules = rulesDataset.take(input.getRuleLimit());
        } else {
            rules = rulesDataset.collect();
        }
        final Broadcast<List<Tuple2<List<Object>, Object>>> brRules = javaSparkContext.broadcast(rules);

        // apply rules
        final StructField itemsField = itemsDataset.schema().apply(input.getItemColumn());
        final int itemsFieldIdx = itemsDataset.schema().fieldIndex(input.getItemColumn());
        final StructType outputSchema = itemsDataset.schema().add(input.getOutputColumn(), itemsField.dataType(), false);
        final Dataset<Row> result = itemsDataset
            .mapPartitions(new RulesApplyFunction(itemsFieldIdx, brRules), RowEncoder.apply(outputSchema));
        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), result);

        LOGGER.info("Association rules apply job done.");

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
     * @author Bjoern Lohrmann, KNIME GmbH
     */
    public class RulesApplyFunction implements MapPartitionsFunction<Row, Row> {
        private static final long serialVersionUID = 1L;
        private final int m_inputColIndex;
        private final Broadcast<List<Tuple2<List<Object>, Object>>> m_brRules;

        /**
         * Default constructor.
         *
         * @param inputColIndex collection column index in input dataset
         * @param brRules tuple with antecedent items list and a consequent item
         */
        public RulesApplyFunction(final int inputColIndex, final Broadcast<List<Tuple2<List<Object>, Object>>> brRules) {
            m_inputColIndex = inputColIndex;
            m_brRules = brRules;
        }

        @Override
        public Iterator<Row> call(final Iterator<Row> inputRows) throws Exception {
            final HashSet<Object> result = new HashSet<>();
            final List<Tuple2<List<Object>, Object>> rules = m_brRules.getValue();

            return new Iterator<Row>() {
                @Override
                public boolean hasNext() {
                    return inputRows.hasNext();
                }

                @Override
                public Row next() {
                    final Row inputRow = inputRows.next();
                    final RowBuilder rb = RowBuilder.fromRow(inputRow);
                    final List<Object> items = inputRow.getList(m_inputColIndex);

                    if (items != null) {
                        for (Tuple2<List<Object>, Object> rule : rules) {
                            applyRule(items, rule, result);
                        }
                        rb.add(result.toArray());
                        result.clear();
                    } else {
                        rb.add(new Object[0]);
                    }
                    return rb.build();
                }
            };
        }

        /**
         * If input items contains all antecedent items of the rule, add all new consequent items to the result set.
         *
         * @param items input item set
         * @param rule tuple with antecedent and consequent item lists
         * @param result predicted items set
         */
        private void applyRule(final List<Object> items, final Tuple2<List<Object>, Object> rule,
            final HashSet<Object> result) {

            final Object o = rule._2();

            if (!result.contains(o) && items.containsAll(rule._1())) {
                if (!items.contains(o)) { // add only new items to prediction
                    result.add(o);
                }
            }
        }
    }
}

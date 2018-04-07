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
package org.knime.bigdata.spark2_2.jobs.mllib.associationrule;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleLearnerJobInput;
import org.knime.bigdata.spark.node.mllib.associationrule.AssociationRuleLearnerJobOutput;
import org.knime.bigdata.spark2_2.api.NamedObjects;
import org.knime.bigdata.spark2_2.api.SparkJob;
import org.knime.bigdata.spark2_2.jobs.mllib.freqitemset.FrequentItemSetModel;

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

        final FrequentItemSetModel freqItemsModel = (FrequentItemSetModel)input.getFreqItemsModel();
        final Dataset<Row> freqItemsets = namedObjects.getDataFrame(freqItemsModel.getFrequentItemsObjectName());
        final String modelUid = freqItemsModel.getModelUid();
        final FPGrowthModel model = new FPGrowthModel(modelUid, freqItemsets);
        model.setMinConfidence(input.getMinConfidence());
        namedObjects.addDataFrame(input.getAssociationRulesOutputObject(), model.associationRules());

        LOGGER.info("Association rules learner done.");

        return new AssociationRuleLearnerJobOutput(
            new AssociationRuleModel(modelUid, input.getAssociationRulesOutputObject(), input.getMinConfidence(), "antecedent", "consequent"));
    }
}

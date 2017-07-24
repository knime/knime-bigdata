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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark2_2.jobs.preproc.tfidf;

import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.preproc.tfidf.TFIDFJobInput;
import com.knime.bigdata.spark2_2.api.NamedObjects;
import com.knime.bigdata.spark2_2.api.SimpleSparkJob;

/**
 * Splits a given string column into a word vector and adds the (rescaled) feature vector as a new column.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SparkClass
public class TFIDFJob implements SimpleSparkJob<TFIDFJobInput> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(TFIDFJob.class.getName());
    private static final String ALG_NAME = "TF-IDF";

    @Override
    public void runJob(final SparkContext sparkContext, final TFIDFJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {

        LOGGER.info("Starting " + ALG_NAME + " job...");

        final Dataset<Row> rowRDD = namedObjects.getDataFrame(input.getFirstNamedInputObject());
        Dataset<Row> tfidf = execute(input, rowRDD);

        namedObjects.addDataFrame(input.getFirstNamedOutputObject(), tfidf);
        LOGGER.info(ALG_NAME + " job done.");
        }

    private Dataset<Row> execute(final TFIDFJobInput config, final Dataset<Row> input) {
        final String inputColumn = input.columns()[config.getColIdx()];
        final String wordsColumn = "words_" + UUID.randomUUID().toString().replace('-', '_');
        final String rawFeaturesColumn = inputColumn + "_RawFeatures";
        final String featuresColumn = inputColumn + "_Features";
        final String separator = config.getTermSeparator();
        final Integer minFrequency = config.getMinFrequency();
        final RegexTokenizer tokenizer = new RegexTokenizer().setInputCol(inputColumn)
                                                             .setOutputCol(wordsColumn)
                                                             .setPattern(separator);
        final Dataset<Row> tokenized = tokenizer.transform(input);

        final HashingTF hashingTF = new HashingTF().setInputCol(wordsColumn)
                                                   .setOutputCol(rawFeaturesColumn);

        if (config.getMaxNumTerms() != null && config.getMaxNumTerms() > 0) {
            final Integer maxNumTerms = config.getMaxNumTerms();
            hashingTF.setNumFeatures(maxNumTerms);
        }

        final Dataset<Row> featurizedData = hashingTF.transform(tokenized);
        featurizedData.cache();
        final IDF idf = new IDF().setInputCol(rawFeaturesColumn)
                           .setOutputCol(featuresColumn)
                           .setMinDocFreq(minFrequency);
        final IDFModel idfModel = idf.fit(featurizedData);
        final Dataset<Row> rescaledData = idfModel.transform(featurizedData);

        return rescaledData.drop(wordsColumn, rawFeaturesColumn);
    }
}

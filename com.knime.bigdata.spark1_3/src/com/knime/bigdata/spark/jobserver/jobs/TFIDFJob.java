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
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 * splits a given string column into a word vector and adds the vector to an RDD
 *
 * @author dwk
 */
public class TFIDFJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(TFIDFJob.class.getName());

    /**
     * index of column to be split into word vector
     */
    public static final String PARAM_COL_INDEX = "colIndex";

    /**
     * term separator
     */
    public static final String PARAM_SEPARATOR = "separator";

    /**
     * minimal term frequency for a term to be kept
     */
    public static final String PARAM_MIN_FREQUENCY = "minFrequency";

    /**
     * maximal number of terms to be kept (optional param)
     */
    public static final String PARAM_MAX_NUM_TERMS = "maxNumTerms";


    Logger getLogger() {
        return LOGGER;
    }

    String getAlgName() {
        return "TF-IDF";
    }

    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_COL_INDEX)) {
            msg = "Input parameter '" + PARAM_COL_INDEX + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_SEPARATOR)) {
            msg = "Input parameter '" + PARAM_SEPARATOR + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(PARAM_MIN_FREQUENCY)) {
            msg = "Input parameter '" + PARAM_MIN_FREQUENCY + "' missing.";
        }

        if (msg == null && !aConfig.hasInputParameter(KnimeSparkJob.PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + KnimeSparkJob.PARAM_INPUT_TABLE + "' missing.";
        }
        if (msg == null && !aConfig.hasOutputParameter(KnimeSparkJob.PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + KnimeSparkJob.PARAM_RESULT_TABLE + "' missing.";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }

        return ValidationResultConverter.valid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JobResult runJobWithContext(final SparkContext aSparkContext, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, getLogger());
        getLogger().log(Level.INFO, "START " + getAlgName() + " job...");

        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        JavaRDD<Row> tfidf = execute(aConfig, rowRDD);

        addToNamedRdds(aConfig.getOutputStringParameter(KnimeSparkJob.PARAM_RESULT_TABLE), tfidf);

        getLogger().log(Level.INFO, "DONE " + getAlgName() + " job...");
        return JobResult.emptyJobResult().withMessage("OK");
    }

    static JavaRDD<Row> execute(final JobConfig aConfig, final JavaRDD<Row> aRowRDD) {

        final Integer stringCol = aConfig.getInputParameter(PARAM_COL_INDEX, Integer.class);
        final String separator = aConfig.getInputParameter(PARAM_SEPARATOR);
        final Integer minFrequency = aConfig.getInputParameter(PARAM_MIN_FREQUENCY, Integer.class);

        final JavaRDD<Iterable<String>> text = aRowRDD.map(new Function<Row, Iterable<String>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public ArrayList<String> call(final Row aRow) throws Exception {

                final String[] words;
                if (!aRow.isNullAt(stringCol)) {
                    words = aRow.getString(stringCol).split(separator);
                } else {
                    words = new String[0];
                }
                ArrayList<String> a = new ArrayList<>();
                for (String w : words) {
                    a.add(w);
                }
                return a;
            }
        });

        final HashingTF hashingTF;
        if (aConfig.hasInputParameter(PARAM_MAX_NUM_TERMS) && (Integer)aConfig.getInputParameter(PARAM_MAX_NUM_TERMS, Integer.class) > 0) {
            final Integer maxNumTerms = aConfig.getInputParameter(PARAM_MAX_NUM_TERMS, Integer.class);
            hashingTF = new HashingTF(maxNumTerms);
        } else {
            hashingTF = new HashingTF();
        }
        final JavaRDD<Vector> tf = hashingTF.transform(text);

        //While applying HashingTF only needs a single pass to the data, applying IDF needs two passes: first to compute the IDF vector and second to scale the term frequencies by IDF.
        //MLlib’s IDF implementation provides an option for ignoring terms which occur in less than a minimum number of documents.
        // In such cases, the IDF for these terms is set to 0. This feature can be used by passing the minDocFreq value to the IDF constructor.
        tf.cache();
        final IDFModel idf = new IDF(minFrequency).fit(tf);
        JavaRDD<Vector> tfidf =  idf.transform(tf);

        return RDDUtils.addColumn(aRowRDD.zip(tfidf));
    }
}

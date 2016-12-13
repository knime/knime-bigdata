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
package com.knime.bigdata.spark2_0.jobs.preproc.tfidf;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.node.preproc.tfidf.TFIDFJobInput;
import com.knime.bigdata.spark2_0.api.NamedObjects;
import com.knime.bigdata.spark2_0.api.SimpleSparkJob;

/**
 * splits a given string column into a word vector and adds the vector to an RDD
 *
 * @author dwk
 */
@SparkClass
public class TFIDFJob implements SimpleSparkJob<TFIDFJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(TFIDFJob.class.getName());


    Logger getLogger() {
        return LOGGER;
    }

    String getAlgName() {
        return "TF-IDF";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void runJob(final SparkContext sparkContext, final TFIDFJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {
        getLogger().info("START " + getAlgName() + " job...");

        final JavaRDD<Row> rowRDD = namedObjects.getJavaRdd(input.getFirstNamedInputObject());
        JavaRDD<Row> tfidf = execute(input, rowRDD);

        namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), tfidf);
        getLogger().info("DONE " + getAlgName() + " job...");
    }

    static JavaRDD<Row> execute(final TFIDFJobInput input, final JavaRDD<Row> aRowRDD) {

        final Integer stringCol = input.getColIdx();
        final String separator = input.getTermSeparator();
        final Integer minFrequency = input.getMinFrequency();

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
        if (input.getMaxNumTerms() != null && input.getMaxNumTerms() > 0) {
            final Integer maxNumTerms = input.getMaxNumTerms();
            hashingTF = new HashingTF(maxNumTerms);
        } else {
            hashingTF = new HashingTF();
        }
        final JavaRDD<Vector> tf = hashingTF.transform(text);

        //While applying HashingTF only needs a single pass to the data, applying IDF needs two passes: first to compute the IDF vector and second to scale the term frequencies by IDF.
        //MLlib�s IDF implementation provides an option for ignoring terms which occur in less than a minimum number of documents.
        // In such cases, the IDF for these terms is set to 0. This feature can be used by passing the minDocFreq value to the IDF constructor.
        tf.cache();
        final IDFModel idf = new IDF(minFrequency).fit(tf);
        JavaRDD<Vector> tfidf =  idf.transform(tf);

        return RDDUtils.addColumn(aRowRDD.zip(tfidf));
    }
}

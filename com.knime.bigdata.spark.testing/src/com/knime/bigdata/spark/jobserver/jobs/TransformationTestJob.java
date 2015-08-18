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
 *   Created on 29.05.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import scala.Tuple2;
import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.UserDefinedTransformation;

/**
 *
 * @author dwk
 */
public class TransformationTestJob extends KnimeSparkJob {

    private final static Logger LOGGER = Logger.getLogger(TransformationTestJob.class.getName());

    /**
     * parse parameters - there are no default values, but two required values: - the key of the input JavaRDD - the key
     * of the output JavaRDD
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }
        if (msg == null && !aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private SparkJobValidation validateInput(final JobConfig aConfig) {
        String msg = null;
        if (!validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE))) {
            msg = "Input data table missing for key: " + PARAM_INPUT_TABLE;
        }
        if (msg != null) {
            LOGGER.severe(msg);
            return ValidationResultConverter.invalid(GenericKnimeSparkException.ERROR + ": " + msg);
        }

        return ValidationResultConverter.valid();
    }

    /**
     * run the actual job, the result is serialized back to the client the primary result of this job should be a side
     * effect - new new RDD in the map of named RDDs
     *
     * @return JobResult with table information
     */
    @Override
    protected JobResult runJobWithContext(final SparkContext aSparkContext, final JobConfig aConfig) {
        SparkJobValidation validation = validateInput(aConfig);
        if (!ValidationResultConverter.isValid(validation)) {
            return JobResult.emptyJobResult().withMessage(validation.toString());
        }

        LOGGER.log(Level.INFO, "starting transformation job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));

        final JavaRDD<Row> transformed = new MyTransformer().apply(rowRDD, rowRDD);

        LOGGER.log(Level.INFO, "transformation completed");
        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), transformed);
        try {
            final StructType schema = StructTypeBuilder.fromRows(transformed.take(10)).build();
            return JobResult.emptyJobResult().withMessage("OK")
                .withTable(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), schema);
        } catch (InvalidSchemaException e) {
            return JobResult.emptyJobResult().withMessage("ERROR: " + e.getMessage());
        }
    }

    /**
     *
     * note that this must be a static inner class - otherwise, Spark will
     * throw a ClassNotFoundException
     *
     * @author dwk
     */
    private static class MyTransformer implements UserDefinedTransformation {
        private static final long serialVersionUID = 1L;

        @Override
        @Nonnull
        public <T extends JavaRDD<Row>> JavaRDD<Row> apply(@Nonnull final T aInput1, final T aInput2) {

            JavaPairRDD<String, Row> pair1 = aInput1.mapToPair(new PairFunction<Row, String, Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<String, Row> call(final Row arg0) throws Exception {
                    return new Tuple2<String, Row>(arg0.getString(4), arg0);
                }
            });
            JavaPairRDD<String, Row> pair2 = aInput2.mapToPair(new PairFunction<Row, String, Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<String, Row> call(final Row arg0) throws Exception {
                    return new Tuple2<String, Row>(arg0.getString(4), arg0);
                }
            });

            JavaPairRDD<String, Tuple2<Row, Row>> result = pair1.join(pair2);
            result.map(new Function<Tuple2<String,Tuple2<Row,Row>>, Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Row call(final Tuple2<String, Tuple2<Row, Row>> arg0) throws Exception {
                    RowBuilder builder = RowBuilder.fromRow(arg0._2._1);
                    for (int i = 0; i < arg0._2._2.length(); ++i) {
                        builder.add(arg0._2._2.get(i));
                      }
                    return builder.build();
                }
            });

            //aggregate by class
            JavaPairRDD<String, Iterable<Row>> t = aInput1.groupBy(new Function<Row, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String call(final Row aRow) throws Exception {
                    //4th column (0-based) is the class column
                    return aRow.getString(4);
                }
            });

            //now add the class count to each row
            return t.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Row>>, Row>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Iterable<Row> call(final Tuple2<String, Iterable<Row>> rowsWithSameLabel) throws Exception {
                    //count number of elements
                    Iterator<Row> l = rowsWithSameLabel._2.iterator();
                    int ctr = 0;
                    while (l.hasNext()) {
                        ctr++;
                        l.next();
                    }

                    //add count to each row
                    l = rowsWithSameLabel._2.iterator();
                    List<Row> res = new ArrayList<>();
                    while (l.hasNext()) {
                        res.add(RowBuilder.fromRow(l.next()).add(ctr).build());
                    }
                    return res;
                }
            });
        }
    }
}

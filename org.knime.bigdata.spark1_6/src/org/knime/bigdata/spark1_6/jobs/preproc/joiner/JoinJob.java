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
 *   Created on Feb 13, 2015 by koetter
 */
package org.knime.bigdata.spark1_6.jobs.preproc.joiner;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.google.common.base.Optional;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.job.util.MyJoinKey;
import org.knime.bigdata.spark.node.preproc.joiner.JoinMode;
import org.knime.bigdata.spark.node.preproc.joiner.SparkJoinerJobInput;
import org.knime.bigdata.spark1_6.api.NamedObjects;
import org.knime.bigdata.spark1_6.api.RDDUtilsInJava;
import org.knime.bigdata.spark1_6.api.SimpleSparkJob;

import scala.Tuple2;

/**
 * executes join of two JavaRDD<Row> and puts result into a JavaRDD<Row>
 *
 * @author dwk
 */
@SparkClass
public class JoinJob implements SimpleSparkJob<SparkJoinerJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(JoinJob.class.getName());


    /**
     * run the actual job, the result is serialized back to the client the true result is stored in the map of named
     * RDDs
     *
     * @throws KNIMESparkException
     */
    @Override
    public void runJob(final SparkContext sparkcontext, final SparkJoinerJobInput input, final NamedObjects namedObjects)
        throws KNIMESparkException {
        final JoinMode mode = input.getJoineMode();
        LOGGER.info("computing " + mode.toString() + " of two RDDs...");

        final List<Integer> joinIdxLeft = Arrays.asList(input.getJoinColIdxsLeft());
        JavaPairRDD<MyJoinKey, Row> leftRdd =
            RDDUtilsInJava.extractKeys(namedObjects.getJavaRdd(input.getLeftInputObject()),
                joinIdxLeft.toArray(new Integer[joinIdxLeft.size()]));
        final List<Integer> joinIdxRight = Arrays.asList(input.getJoinColIdxsRight());
        JavaPairRDD<MyJoinKey, Row> rightRdd =
            RDDUtilsInJava.extractKeys(namedObjects.getJavaRdd(input.getRightNamedObject()),
                joinIdxRight.toArray(new Integer[joinIdxRight.size()]));

        final List<Integer> colIdxLeft = Arrays.asList(input.getSelectColIdxsLeft());
        final List<Integer> colIdxRight = Arrays.asList(input.getSelectColIdxsRight());

        //printRDD(leftRdd.collect(), "Left table:");
        //printRDD(rightRdd.collect(), "Right table:");
        final JavaRDD<Row> resultRdd;
        switch (mode) {
            case InnerJoin: {
                JavaRDD<Tuple2<Row, Row>> joinedRdd = leftRdd.join(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case LeftOuterJoin: {
                JavaRDD<Tuple2<Row, Optional<Row>>> joinedRdd = leftRdd.leftOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case RightOuterJoin: {
                JavaRDD<Tuple2<Optional<Row>, Row>> joinedRdd = leftRdd.rightOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            case FullOuterJoin: {
                JavaRDD<Tuple2<Optional<Row>, Optional<Row>>> joinedRdd = leftRdd.fullOuterJoin(rightRdd).values();
                resultRdd = RDDUtilsInJava.mergeRows(joinedRdd, colIdxLeft, colIdxRight);
                break;
            }
            default: {
                throw new KNIMESparkException("ERROR: unsupported join mode: " + mode);
            }
        }

        //printJoinedRDD(joinedRdd.collect(), "Joined table:");

        //printSelectedRDD(resultRdd.collect(), "Result table:");

        LOGGER.info("done");

        LOGGER.info("Storing join result under key: " + input.getFirstNamedOutputObject());
        namedObjects.addJavaRdd(input.getFirstNamedOutputObject(), resultRdd);
    }

//    /**
//     * @param collect
//     * @param string
//     */
//    @SuppressWarnings("unused")
//    private void printSelectedRDD(final List<Row> aRdd, final String aMsg) {
//        LOGGER.log(Level.INFO, aMsg);
//        for (Row tuple : aRdd) {
//            LOGGER.log(Level.INFO, tuple.toString());
//        }
//        LOGGER.log(Level.INFO, "<---- END OF TABLE");
//    }
//
//    /**
//     * @param collect
//     * @param aMsg
//     */
//    @SuppressWarnings("unused")
//    private void printJoinedRDD(final List<Tuple2<String, Tuple2<Row, Row>>> aRdd, final String aMsg) {
//        LOGGER.log(Level.INFO, aMsg);
//        for (Tuple2<String, Tuple2<Row, Row>> tuple : aRdd) {
//            LOGGER.log(Level.INFO, "keys:\t" + (tuple._1) + "\tvalues: " + tuple._2);
//        }
//        LOGGER.log(Level.INFO, "<---- END OF TABLE");
//    }
//
//    /**
//     * @param collect
//     */
//    @SuppressWarnings("unused")
//    private void printRDD(final List<Tuple2<String, Row>> aRdd, final String aMsg) {
//        LOGGER.log(Level.INFO, aMsg);
//        for (Tuple2<String, Row> tuple : aRdd) {
//            LOGGER.log(Level.INFO, "keys:\t" + (tuple._1) + "\tvalues: " + tuple._2);
//        }
//        LOGGER.log(Level.INFO, "<---- END OF TABLE");
//    }

}

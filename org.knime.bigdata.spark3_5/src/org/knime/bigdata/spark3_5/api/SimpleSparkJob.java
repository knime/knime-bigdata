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
 *   Created on May 3, 2016 by bjoern
 */
package org.knime.bigdata.spark3_5.api;

import java.io.Serializable;

import org.apache.spark.SparkContext;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Interface for simple Spark jobs that only take a {@link JobInput} object, but do not return anything to the client
 * (usually a node).
 *
 * @author Bjoern Lohrmann, KNIME.com
 * @param <I> type of job input
 */
@SparkClass
public interface SimpleSparkJob<I extends JobInput> extends Serializable {

    /**
     * Invoked to run the Spark job.
     *
     * @param sparkContext The context to run the Spark job in
     * @param input Input parameters provided by the client (e.g. the KNIME Analytics Platform GUI)
     * @param namedObjects A handle to manage (get, set, list, ..) named objects such as RDDs
     * @throws KNIMESparkException If something goes wrong during execution. The String returned by
     *             {@link KNIMESparkException#getMessage()} will be displayed prominently to a user, so it should be
     *             instructive or at least provide some explanation. See javadoc in {@link KNIMESparkException} for more
     *             details.
     * @throws Exception If something goes wrong during execution, but no instructive error message can be reported.
     *             This results the exception being logged and a boilerplate error message being shown to the user.
     */
    public void runJob(final SparkContext sparkContext, final I input, final NamedObjects namedObjects)
        throws KNIMESparkException, Exception;

}

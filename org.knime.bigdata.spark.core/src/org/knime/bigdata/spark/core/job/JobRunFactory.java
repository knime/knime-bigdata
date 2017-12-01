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
 *   Created on 22.09.2015 by koetter
 */
package org.knime.bigdata.spark.core.job;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <I> {@link JobInput}
 * @param <O> {@link JobOutput}
 */
public interface JobRunFactory<I extends JobInput, O extends JobOutput> {

    /**
     * @return the unique job id
     */
    public String getJobID();

    /**
     * @param input {@link JobInput}
     * @return {@link JobRun} instance
     */
    public JobRun<I, O> createRun(I input);
}

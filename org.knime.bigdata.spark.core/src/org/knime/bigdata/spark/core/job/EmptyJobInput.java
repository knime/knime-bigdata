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
 *   Created on Apr 15, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.job;

import java.util.Collection;

import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;

/**
 * This class is a immutable {@link JobInput} for jobs that do not need any input.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public final class EmptyJobInput extends JobInput {

    private static final String IMMUTABLE_ERROR = "EmptyJobInput is immutable.";

    private static final EmptyJobInput SINGLETON_INSTANCE = new EmptyJobInput();

    /**
     * Zero parameter constructor for serialization purposes.
     */
    public EmptyJobInput() {
        // empty output, no need to initialize internal map
    }

    /**
     * @return the only instance
     */
    public static EmptyJobInput getInstance() {
        return SINGLETON_INSTANCE;
    }

    @Override
    public void addNamedInputObject(final String name) {
        throw new IllegalAccessError(IMMUTABLE_ERROR);
    }

    @Override
    public void addNamedInputObjects(final Collection<String> names) {
        throw new IllegalAccessError(IMMUTABLE_ERROR);
    }

    @Override
    public void addNamedOutputObject(final String name) {
        throw new IllegalAccessError(IMMUTABLE_ERROR);
    }

    @Override
    public void addNamedOutputObjects(final Collection<String> names) {
        throw new IllegalAccessError(IMMUTABLE_ERROR);
    }

    @Override
    protected <T> T set(final String key, final T value) {
        throw new IllegalAccessError(IMMUTABLE_ERROR);
    }

    @Override
    public <T extends JobData> T withSpec(final String namedObjectId, final IntermediateSpec spec) {
        throw new IllegalAccessError(IMMUTABLE_ERROR);
    }
}

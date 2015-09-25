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
 *   Created on 11.09.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;

/**
 *
 * @author dwk
 */
public class BoundedMultiVariateStatisticalSummary implements Serializable, MultivariateStatisticalSummary {

    private static final long serialVersionUID = 1L;

    // maximal number of values to store
    private final static int BOUND = 1000;

    private final long m_count;

    private final Vector m_max;

    private final Vector m_mean;

    private final Vector m_min;

    private final Vector m_normL1;

    private final Vector m_normL2;

    private final Vector m_numNonzeros;

    private final Vector m_variance;

    /**
     * create a bounded copy of the given statistics
     * @param aSource
     */
    public BoundedMultiVariateStatisticalSummary(final MultivariateStatisticalSummary aSource) {
        m_count = aSource.count();
        m_min = copyVector(aSource.min(), BOUND);
        m_max = copyVector(aSource.max(), BOUND);
        m_mean = copyVector(aSource.mean(), BOUND);
        m_variance = copyVector(aSource.variance(), BOUND);
        m_normL1 = copyVector(aSource.normL1(), BOUND);
        m_normL2 = copyVector(aSource.normL2(), BOUND);
        m_numNonzeros = copyVector(aSource.numNonzeros(), BOUND);
    }

    /**
     * @param aVector
     * @param aMaxLength
     * @return possibly truncated copy
     */
    private Vector copyVector(final Vector aVector, final int aMaxLength) {
        if (aVector.size() > aMaxLength) {
            final double[] subSelect = Arrays.copyOf(aVector.toArray(), aMaxLength);
            return Vectors.dense(subSelect);
        }
        return aVector.copy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long count() {
        return m_count;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector max() {
        return m_max;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector mean() {
        return m_mean;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector min() {
        return m_min;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector normL1() {
        return m_normL1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector normL2() {
        return m_normL2;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector numNonzeros() {
        return m_numNonzeros;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector variance() {
        return m_variance;
    }

}

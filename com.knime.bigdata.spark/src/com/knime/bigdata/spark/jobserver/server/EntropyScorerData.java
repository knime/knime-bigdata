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
 *   Created on 22.09.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;



/**
 * Utility class that stores some entropy and quality values for clustering results given a reference
 * clustering.
 *
 * @author Bernd Wiswedel, University of Konstanz, dwk
 */
public class EntropyScorerData implements Serializable {

    private static final long serialVersionUID = 1L;

    private final double m_entropy;

    private final double m_quality;

    private final int m_nrClusters;

    private final int m_nrReferenceClusters;


    /**
     * stores values
     * @param aEntropy
     * @param aQuality
     * @param aNrClusters
     * @param aNrReferenceClusters
     */
    public EntropyScorerData(final double aEntropy, final double aQuality, final int aNrClusters, final int aNrReferenceClusters) {
        m_entropy = aEntropy;
        m_quality = aQuality;
        m_nrClusters = aNrClusters;
        m_nrReferenceClusters = aNrReferenceClusters;
    }

    /**
     * @return the entropy
     */
    public double getEntropy() {
        return m_entropy;
    }

    /**
     * @return the quality
     */
    public double getQuality() {
        return m_quality;
    }

    /**
     * @return the nrClusters
     */
    public int getNrClusters() {
        return m_nrClusters;
    }

    /**
     * @return the nrReference
     */
    public int getNrReferenceClusters() {
        return m_nrReferenceClusters;
    }

    /**
     * @return
     */
    public int getOverallSize() {
        // FIXME
        return 0;
    }

    /**
     * @return
     */
    public double getOverallNormalizedEntropy() {
        // FIXME
        return 0;
    }
}

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
 *   Created on 14.03.2016 by koetter
 */
package com.knime.bigdata.spark.core.version;

import org.osgi.framework.Version;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkVersion extends Version {

    /**Spark version 1.2.0.*/
    public static final SparkVersion V_1_2 = new SparkVersion(1, 2, 0);

    /**Spark version 1.3.0.*/
    public static final SparkVersion V_1_3 = new SparkVersion(1, 3, 0);

    /**Spark version 1.5.0.*/
    public static final SparkVersion V_1_5 = new SparkVersion(1, 5, 0);

    /**Spark version 1.6.0.*/
    public static final SparkVersion V_1_6 = new SparkVersion(1, 6, 0);

    /**All {@link SparkVersion}s.*/
    public static final SparkVersion[] ALL = new SparkVersion[]{V_1_2, V_1_3, V_1_5, V_1_6};

    /**
     * Creates a Spark version identifier from the specified numerical components.
     *
     * <p>
     * The qualifier is set to the empty string.
     *
     * @param major Major component of the version identifier.
     * @param minor Minor component of the version identifier.
     * @param micro Micro component of the version identifier.
     * @throws IllegalArgumentException If the numerical components are
     *         negative.
     */
    public SparkVersion(final int major, final int minor, final int micro) {
        super(major, minor, micro);
    }

    /**
     * @param version
     */
    public SparkVersion(final String version) {
        super(version);
    }

    /**
     * @param label the {@link String} representation of the {@link SparkVersion}
     * @return the {@link SparkVersion} for the given label
     */
    public static SparkVersion getVersion(final String label) {
        return new SparkVersion(label);
    }

    /**
     * @return the string representation of the {@link SparkVersion}
     */
    public String getLabel() {
        return String.format("%d.%d", getMajor(), getMinor());
    }

    /**
     * @return all version labels.
     */
    public static String[] getAllVersionLabels() {
        String labels[] = new String[ALL.length];
        for (int i = 0; i < ALL.length; i++) {
            labels[i] = ALL[i].getLabel();
        }
        return labels;
    }
}

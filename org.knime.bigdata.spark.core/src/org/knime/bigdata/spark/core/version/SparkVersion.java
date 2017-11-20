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
 *   Created on 14.03.2016 by koetter
 */
package com.knime.bigdata.spark.core.version;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.osgi.framework.Version;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkVersion extends Version {

    /**
     * A lookup map from string representation (.toString()) to Spark version.
     */
    private final static Map<String, SparkVersion> VERSION_MAP = new HashMap<>();

    /** Spark version 1.2.0. */
    public static final SparkVersion V_1_2 = new SparkVersion(1, 2);

    /** Spark version 1.3.0. */
    public static final SparkVersion V_1_3 = new SparkVersion(1, 3);

    /** Spark version 1.5.0. */
    public static final SparkVersion V_1_5 = new SparkVersion(1, 5);

    /** Spark version 1.6.0. */
    public static final SparkVersion V_1_6 = new SparkVersion(1, 6);

    /** Spark version 1.6 on Cloudera CDH 5.9 and later */
    public static final SparkVersion V_1_6_CDH_5_9 = new SparkVersion(1, 6, 0, "cdh5_9", "1.6 (CDH 5.9+)");

    /** Spark version 2.0.0. */
    public static final SparkVersion V_2_0 = new SparkVersion(2, 0);

    /** Spark version 2.1.0. */
    public static final SparkVersion V_2_1 = new SparkVersion(2, 1);

    /** Spark version 2.2.0. */
    public static final SparkVersion V_2_2 = new SparkVersion(2, 2);

    /** All {@link SparkVersion}s. */
    public static final SparkVersion[] ALL = new SparkVersion[]{V_1_2, V_1_3, V_1_5, V_1_6, V_1_6_CDH_5_9, V_2_0, V_2_1, V_2_2};

    /**
     * Label for display purposes in GUI.
     */
    private final String m_label;

    /**
     * Creates a Spark version identifier from the specified numerical components. It will have "major.minor" as label
     * for display purposes.
     *
     * <p>
     * The qualifier is set to the empty string.
     *
     * @param major Major component of the version identifier.
     * @param minor Minor component of the version identifier.
     * @throws IllegalArgumentException If the numerical components are negative.
     */
    protected SparkVersion(final int major, final int minor) {
        this(major, minor, 0, null, String.format("%d.%d", major, minor));
    }

    /**
     * Creates a Spark version identifier from the specified components. This constructor allows to set a label for GUI
     * display purposes.
     *
     * @param major Major component of the version identifier.
     * @param minor Minor component of the version identifier.
     * @param micro Micro component of the version identifier.
     * @param qualifier Qualifier string. May be null.
     * @param label Label for GUI display purposes. Must not be null.
     * @throws IllegalArgumentException If the numerical components are negative or the label was null.
     */
    protected SparkVersion(final int major, final int minor, final int micro, final String qualifier,
        final String label) {
        super(major, minor, micro, qualifier);

        if (label == null) {
            throw new IllegalArgumentException("Label must not be null");
        }

        m_label = label;

        VERSION_MAP.put(toString(), this);
    }

    /**
     *
     * @param stringRepr the {@link String} representation of the {@link SparkVersion}
     * @return the {@link SparkVersion} for the given string representation
     */
    public static SparkVersion fromString(final String stringRepr) {
        return VERSION_MAP.get(stringRepr);
    }

    /**
     * @param label The GUI label string of the {@link SparkVersion}
     * @return the {@link SparkVersion} for the given label
     */
    public static SparkVersion fromLabel(final String label) {
        return ALL[Arrays.asList(getAllVersionLabels()).indexOf(label)];
    }

    /**
     * @return A label string to be used for representing the Spark version in the GUI.
     */
    public String getLabel() {
        return m_label;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(getMajor());
        buf.append('.');
        buf.append(getMinor());

        if (getMicro() != 0) {
            buf.append('.');
            buf.append(getMicro());
        }

        if (getQualifier().length() > 0) {
            buf.append('.');
            buf.append(getQualifier());
        }

        return buf.toString();
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

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
 *   Created on Sep 1, 2017 by bjoern
 */
package com.knime.bigdata.spark.core.version;

import java.util.HashMap;

import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.SparkPlugin;

/**
 * Provides and manages OSGI versions of KNIME Spark Executor.
 *
 * @author Bjoern Lohrmann, KNIME
 * @since 2.1.0
 */
public class SparkPluginVersion {

    /**
     * Holds all instances of Spark plugin versions.
     */
    private final static HashMap<String, Version> VERSION_MAP = new HashMap<>();

    /**
     * Public constant for version 2.0.1 of KNIME Spark Executor plugin.
     *
     */
    public final static Version VERSION_2_0_1 = new Version(2, 0, 1);

    /**
     * Public constant for version 2.1.0 of KNIME Spark Executor plugin.
     *
     */
    public final static Version VERSION_2_1_0 = new Version(2, 1, 0);

    /**
     * Dummy OSGI version representing the equivalent of "zero", i.e. a version that is lower than any actual version of
     * KNIME Spark Executor during {@link Version#compareTo(Version)}.
     *
     */
    public final static Version VERSION_ZERO = new Version(0, 0, 0);

    /**
     * Dummy OSGI version representing the equivalent of "infinity", i.e. a version that is higher than any actual
     * version of KNIME Spark Executor during {@link Version#compareTo(Version)}.
     *
     */
    public final static Version VERSION_INFINITY = new Version(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);

    static {
        // add versions that have been exported as a constants to the version map
        synchronized (VERSION_MAP) {
            VERSION_MAP.put(VERSION_ZERO.toString(), VERSION_ZERO);
            VERSION_MAP.put(VERSION_INFINITY.toString(), VERSION_INFINITY);
            VERSION_MAP.put(VERSION_2_0_1.toString(), VERSION_2_0_1);
            VERSION_MAP.put(VERSION_2_1_0.toString(), VERSION_2_1_0);
        }
    }

    /**
     * Current OSGI version of KNIME Spark Executor plugin.
     *
     */
    public final static Version VERSION_CURRENT = fromString(SparkPlugin.getDefault().getBundle().getVersion().toString());

    /**
     * Returns an OSGI {@link Version} object for the given OSGI version string. Note that the qualifier part of the
     * version string is masked out. This method ensures that equal version strings (after masking) return the same
     * {@link Version} object. Thus the returned objects can be compared with == to determine equality.
     *
     * @param versionString An OSGI {@link Version} of KNIME Spark Executor represented as a string.
     * @return an OSGI {@link Version}
     * @throws IllegalArgumentException if an invalid version String was supplied.
     */
    public static Version fromString(final String versionString) {
        final String maskedVersionString = maskOutQualifier(versionString);

        Version version;
        synchronized (VERSION_MAP) {
            version = VERSION_MAP.get(maskedVersionString);
            if (version == null) {
                version = Version.valueOf(maskedVersionString);
                VERSION_MAP.put(maskedVersionString, version);
            }
        }
        return version;
    }

    /**
     * trims and masks out the build qualifier of the given version string. Used to "canonicalize" versions.
     *
     * @param versionString Version string to be masked.
     * @return a masked version string.
     */
    private static String maskOutQualifier(final String versionString) {
        final String[] parts = versionString.trim().split("\\.");
        if (parts.length == 4) {
            return String.format("%s.%s.%s", parts[0], parts[1], parts[2]);
        } else if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException(
                "Illegal version string for KNIME Spark Executor version: " + versionString);
        } else {
            return versionString;
        }
    }
}

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
 *   Created on 04.03.2016 by koetter
 */
package org.knime.bigdata.spark.core.jar;

import java.io.File;
import java.io.InputStream;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarEntry;

/**
 * Class that allows to assemble a jar file by adding Files and {@link JarEntry} to it.
 *
 * @author Tobias Koetter, KNIME.com
 */
public interface JarCollector {

    /**
     * Returns the KNIME job jar file with all added entries. No more entries can be added to the jar file once this
     * method is called!
     *
     * @return the final jar file
     */
    JobJar getJobJar();

    /**
     * Copies the complete content of the given jar file into the Spark job server jar except for the manifest file.
     *
     * @param jar the complete jar file to add to the existing jar
     */
    void addJar(File jar);

    /**
     * Copies the content of the given jar file minus the entries that match the given filter predicates into the Spark
     * job server jar.
     *
     * @param jar the complete jar file to add to the existing jar
     * @param filterPredicates Predicates against which each jar entry is tested. Only those entries are copied, where
     *            {@link Predicate#test(Object)} returns false for all predicates. May be null.
     */
    void addJar(final File jar, Set<Predicate<JarEntry>> filterPredicates);

    /**
     * Copies the complete content of the directory to the jar file.
     *
     * @param dir the directory to copy into the jar file
     */
    void addDirectory(File dir);

    /**
     * Adds a single file to the jar.
     *
     * @param entryName jar file entry name to use for the file
     * @param file the file to add to the jar
     */
    void addFile(String entryName, File file);

    /**
     * Adds a single {@link JarEntry} to the jar.
     *
     * @param je the {@link JarEntry} to add
     * @param is the {@link InputStream} to read the content of the {@link JarEntry} from
     */
    void addJarEntry(JarEntry je, InputStream is);

    /**
     * Sets the class that is used on Spark jobserver to run Spark jobs.
     *
     * @param jobserverJobClass
     */
    void setJobserverJobClass(String jobserverJobClass);


    /**
     * Adds the ID of a jar provider to the job jar descriptor.
     *
     * @since 1.6.0.20160803 (i.e. added as part of issue BD-175 *after* the July 2016 release)
     * @param providerID
     */
    void addProviderID(final String providerID);
}
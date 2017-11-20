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
 *   Created on 22.03.2016 by koetter
 */
package org.knime.bigdata.spark.core.jar;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.osgi.internal.loader.EquinoxClassLoader;
import org.eclipse.osgi.internal.loader.classpath.ClasspathEntry;
import org.eclipse.osgi.internal.loader.classpath.ClasspathManager;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Default provider
 *
 * @author Tobias Koetter, KNIME.com
 */
@SuppressWarnings("restriction")
public class DefaultSparkJarProvider implements SparkJarProvider {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DefaultSparkJarProvider.class);

    private final CompatibilityChecker m_checker;

    private final Set<Predicate<String>> m_shouldScanPredicates;

    /**
     * Creates a jar provider that only matches the given Spark version and only scans directories/jar files that match
     * at least one of the given {@link Predicate}s.
     *
     * @param sparkVersion the Spark version that is supported by this provider e.g. 1.2, 1.3, etc
     * @param shouldScanPredicates {@link Predicate}s that determine which directories/jars should be scanned for
     *            classes to include.
     */
    @SafeVarargs
    public DefaultSparkJarProvider(final SparkVersion sparkVersion, final Predicate<String>... shouldScanPredicates) {
        this(new FixedVersionCompatibilityChecker(sparkVersion), shouldScanPredicates);
    }

    /**
     * Creates a jar provider that matches all spark versions the given checker supports and only scans directories/jar
     * files that match at least one of the given {@link Predicate}s.
     *
     * @param checker A {@link CompatibilityChecker} to determine which Spark versions to support with this jar
     *            provider.
     * @param shouldScanPredicates {@link Predicate}s that determine which directories/jars should be scanned for
     *            classes to include.
     */
    @SafeVarargs
    public DefaultSparkJarProvider(final CompatibilityChecker checker,
        final Predicate<String>... shouldScanPredicates) {
        m_checker = checker;

        if (shouldScanPredicates.length > 0) {
            m_shouldScanPredicates = new HashSet<>(Arrays.asList(shouldScanPredicates));
        } else {
            m_shouldScanPredicates = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompatibilityChecker getChecker() {
        return m_checker;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
        return m_checker.supportSpark(sparkVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return m_checker.getSupportedSparkVersions();
    }

    /**
     *
     * @param collector the {@link JarCollector} that collects all java classes that should be send to the Spark cluster
     */
    @Override
    public void collect(final JarCollector collector) {
        final long startTime = System.currentTimeMillis();

        final EquinoxClassLoader cl = (EquinoxClassLoader)getClass().getClassLoader();
        LOGGER.debugWithFormat("Collecting Spark classes from plugin %s:%s", cl.getBundle().getSymbolicName(),
            cl.getBundle().getVersion().toString());

        final ClasspathManager cpm = cl.getClasspathManager();
        final ClasspathEntry[] ce = cpm.getHostClasspathEntries();
        final Set<String> processedFileNames = new HashSet<>(ce.length);
        for (final ClasspathEntry classpathEntry : ce) {
            final File classpathEntryFile = classpathEntry.getBundleFile().getBaseFile();
            if (processedFileNames.add(classpathEntryFile.getPath()) && shouldScan(classpathEntryFile)) {
                if (classpathEntryFile.isDirectory()) {
                    scanDirectory(collector, classpathEntryFile);
                } else if (classpathEntryFile.isFile() && classpathEntryFile.getName().endsWith(".jar")) {
                    scanJar(collector, classpathEntryFile);
                }
            }
        }

        LOGGER.debugWithFormat("Time for for collecting classes: %d millis", System.currentTimeMillis() - startTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProviderID() {
        // this also works (and *must* work) for subclasses of DefaultSparkJarProvider that are not in spark.core
        final Bundle bundle = FrameworkUtil.getBundle(getClass());
        return String.format("%s:%s", bundle.getSymbolicName(), bundle.getVersion().toString());
    }

    /**
     * Invoked during {@link #collect(JarCollector)} to determine whether to scan a given directory or jar file for
     * classes annotated with {@link SparkClass}. Subclasses can override this method to prune the tree of searched
     * directories and jar files. The default implementation always returns true, unless (i) shouldScanPredicates have
     * been specified to the constructor, and (ii) {@link File#getName()} does not match any of the predicates.
     *
     *
     * @param jarFileOrDirectory the {@link File}, which is either a directory or jar file to scan
     * @return <code>true</code> if the jar file or directory should be scanned
     */
    protected boolean shouldScan(final File jarFileOrDirectory) {
        if (m_shouldScanPredicates != null) {
            return hasMatchingShouldScanPredicate(jarFileOrDirectory);
        } else {
            return true;
        }
    }

    private boolean hasMatchingShouldScanPredicate(final File jarFileOrDirectory) {
        for (Predicate<String> predicate : m_shouldScanPredicates) {
            if (predicate.test(jarFileOrDirectory.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Invoked during {@link #collect(JarCollector)} when scanning a jar file for classes annotated with
     * {@link SparkClass}. The default implementation loops through all classes in the jar file and checks if the
     * {@link SparkClass} annotation is present.
     *
     * @param collector the {@link JarCollector} to add classes/jar entries to.
     * @param file a {@link File} object that must point to a valid jar file.
     */
    protected void scanJar(final JarCollector collector, final File file) {
        final String path = file.getPath();
        try (final JarFile jarFile = new JarFile(path)) {
            final Enumeration<JarEntry> e = jarFile.entries();
            final URL[] urls = {new URL("jar:file:" + path + "!/")};
            try (final URLClassLoader cl = URLClassLoader.newInstance(urls, this.getClass().getClassLoader())) {
                while (e.hasMoreElements()) {
                    final JarEntry je = e.nextElement();
                    if (je.isDirectory() || !je.getName().endsWith(".class")) {
                        continue;
                    }
                    final String className = JarPacker.getClassName(je);
                    if (isSparkClass(cl, className)) {
                        if (KNIMEConfigContainer.verboseLogging()) {
                            LOGGER.debug("Adding Spark class jar entry: " + className);
                        }
                        collector.addJarEntry(je, jarFile.getInputStream(je));
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.info("Exception checking Spark jar file " + file.getPath() + " Exception : " + ex.getMessage());
        }
    }

    /**
     * Invoked during {@link #collect(JarCollector)} when scanning a directory for classes annotated with
     * {@link SparkClass}. The default implementation recursively loops through all .class files in the subdirectory
     * "bin" of the given directory, and scans for classes that have the {@link SparkClass} annotation.
     *
     * @param collector the {@link JarCollector} to add classes/jar entries to.
     * @param dir a {@link File} object that must point to a directory
     */
    protected void scanDirectory(final JarCollector collector, final File dir) {
        try (final URLClassLoader cl =
            URLClassLoader.newInstance(new URL[]{dir.toURI().toURL()}, this.getClass().getClassLoader());) {
            for (final String fileName : dir.list()) {
                final File file = new File(dir, fileName);
                final String path = file.getPath();
                if (file.isDirectory() && (path.contains(File.separatorChar + "bin" + File.separatorChar)
                    || path.endsWith(File.separatorChar + "bin"))) {
                    //only look at the bin directories (works for eclipse dev setup)
                    scanDirectory(collector, file);
                } else if (fileName.endsWith(".class")) {
                    final int startIdx = path.indexOf(File.separatorChar + "bin" + File.separatorChar) + 5;
                    final int endIdx = path.length() - 6;
                    final String classFileName = path.substring(startIdx, endIdx);
                    final String className = classFileName.replace(File.separatorChar, '.');
                    if (isSparkClass(cl, className)) {
                      //always use / as path separator in the zip entry name!!!
                      //see https://bugs.openjdk.java.net/browse/JDK-6303716?page=com.atlassian.jira.plugin.system.issuetabpanels:all-tabpanel
                      final String zipEntryName = classFileName.replace(File.separatorChar, '/');
                      if (KNIMEConfigContainer.verboseLogging()) {
                          LOGGER.debug("Adding Spark class file: " + className);
                      }
                      collector.addFile(zipEntryName + ".class", file);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.info("Exception checking Spark directory " + dir.getPath() + " Exception: " + e.getMessage());
        }
    }

    /**
     * THis method is called from the {@link #scanJar(JarCollector, File)} and
     * {@link #scanDirectory(JarCollector, File)} method to check if the class with the given name is a Spark clas
     * and thus should be send to the Spark server.
     * @param cl the {@link ClassLoader} that can load the class with the given className
     * @param className the name of the class with . as separator
     * @return <code>true</code> if the class is a Spark class
     * @throws ClassNotFoundException if the class with the given name could not be faound
     */
    protected boolean isSparkClass(final ClassLoader cl, final String className) throws ClassNotFoundException {
        ////TK_TODO: Here we might want to use ASM (http://asm.ow2.org/) or javassist (http://www.javassist.org/)
        //        collector.addDirectory(dir);
        // if this is a nested class, test the outer class
        final String[] classNameComps = className.split("\\.");
        if (classNameComps[classNameComps.length - 1].contains("$")) {
            classNameComps[classNameComps.length - 1] = classNameComps[classNameComps.length - 1]
                .substring(0, classNameComps[classNameComps.length - 1].indexOf('$'));
        }
        final String mainClassName = StringUtils.join(classNameComps, '.');
        final Class<?> c = cl.loadClass(mainClassName);
        return c.isAnnotationPresent(SparkClass.class);
    }
}

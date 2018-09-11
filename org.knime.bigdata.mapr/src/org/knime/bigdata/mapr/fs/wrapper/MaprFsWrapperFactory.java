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
 *   Created on Sep 9, 2018 by bjoern
 */
package org.knime.bigdata.mapr.fs.wrapper;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.eclipse.osgi.internal.loader.EquinoxClassLoader;
import org.eclipse.osgi.internal.loader.classpath.ClasspathEntry;
import org.eclipse.osgi.internal.loader.classpath.ClasspathManager;
import org.knime.core.node.NodeLogger;

/**
 *
 * @author bjoern
 */
public class MaprFsWrapperFactory {

    private static final String MAPR_HOME = "/opt/mapr";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(MaprFsWrapperFactory.class);

    private static ClassLoader maprClassLoader;

    private static Constructor<?> fileWrapperConstructor;

    private static Constructor<?> connectionWrapperConstructor;

    private static MaprFsConnectionWrapper maprFsConnection;

    private static synchronized ClassLoader getMaprClassLoader() throws IOException {
        if (maprClassLoader == null) {
            maprClassLoader = new URLClassLoader(getJars(MAPR_HOME), ClassLoader.getSystemClassLoader()) {
                @Override
                public Class<?> loadClass(final String name) throws ClassNotFoundException {
                    // we need to intercept loading of the MaprFsXXXWrapper interface classes
                    // because otherwise we cannot assign instances their implementing classes
                    // to a variables of the interface type
                    if (name.equals(MaprFsRemoteFileWrapper.class.getName()) || name.equals(MaprFsConnectionWrapper.class.getName())) {
                        return MaprFsWrapperFactory.class.getClassLoader().loadClass(name);
                    } else {
                        return super.loadClass(name);
                    }
                }
            };
        }
        return maprClassLoader;
    }

    /**
     * @param maprHome
     * @return
     * @throws IOException
     */
    private static URL[] getJars(final String maprHome) throws IOException {
        final List<URL> toReturn = getBundleClasspath();

        Process process = Runtime.getRuntime().exec(new String[]{"/opt/mapr/bin/hadoop", "classpath"});
        List<String> classpath = IOUtils.readLines(process.getInputStream(), Charset.defaultCharset());

        classpath.stream().flatMap(cp -> Arrays.stream(cp.split(":")))
            // filter out yarn and mapreduce jars which we don't need for now
            .filter(globbedPath -> !globbedPath.contains("mapreduce"))
            .flatMap(globbedPath -> applyGlobbing(globbedPath))
            // filter out jars with test classes
            .filter(path -> !path.toString().endsWith("-tests.jar"))
            .map(path -> {
                try {
                    return path.toUri().toURL();
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
            })
        // add all URLs to return list
        .collect(Collectors.toCollection(() -> toReturn));

         return toReturn.toArray(new URL[0]);
    }

    private static List<URL> getBundleClasspath() {
        final List<URL> toReturn = new LinkedList<>();

        final EquinoxClassLoader cl = (EquinoxClassLoader)MaprFsWrapperFactory.class.getClassLoader();
        final ClasspathManager cpm = cl.getClasspathManager();
        final ClasspathEntry[] ce = cpm.getHostClasspathEntries();
        final Set<String> processedFileNames = new HashSet<>(ce.length);

        for (final ClasspathEntry classpathEntry : ce) {
            final File cpFile = classpathEntry.getBundleFile().getBaseFile();
            try {
                if (processedFileNames.add(cpFile.getPath())) {
                    if (cpFile.isDirectory() && new File(cpFile, "bin").exists()) {

                        toReturn.add(new File(cpFile, "bin").toURI().toURL());
                    } else if (cpFile.isFile() && cpFile.getName().endsWith(".jar")) {
                        toReturn.add(cpFile.toURI().toURL());
                    }
                }
            } catch (MalformedURLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }

        return toReturn;
    }

    private static Stream<Path> applyGlobbing(final String globbedPath) {
        if (!globbedPath.contains("*")) {
            return Collections.singleton(Paths.get(globbedPath)).stream();
        } else {
            final int lastSeparator = globbedPath.lastIndexOf(File.separator);

            try {
                return StreamSupport
                    .stream(Files.newDirectoryStream(Paths.get(globbedPath.substring(0, lastSeparator + 1)),
                        globbedPath.substring(lastSeparator + 1)).spliterator(), false);
            } catch (IOException e) {
                // ignore
                return Collections.<Path> emptyList().stream();
            }
        }
    }

    private static synchronized Constructor<?> getFileWrapperConstructor()
        throws SecurityException, ClassNotFoundException, IOException {
        if (fileWrapperConstructor == null) {
            fileWrapperConstructor =
                getMaprClassLoader().loadClass(MaprFsRemoteFileWrapperImpl.class.getName()).getConstructors()[0];
        }
        return fileWrapperConstructor;
    }

    private static synchronized Constructor<?> getConnectionWrapperConstructor()
        throws SecurityException, ClassNotFoundException, IOException {
        if (connectionWrapperConstructor == null) {
            connectionWrapperConstructor =
                getMaprClassLoader().loadClass(MaprFsConnectionWrapperImpl.class.getName()).getConstructors()[0];
        }
        return connectionWrapperConstructor;
    }

    public static MaprFsRemoteFileWrapper createRemoteFileWrapper(final URI uri, final boolean useKerberos)
        throws IOException {

        try {
            return (MaprFsRemoteFileWrapper) getFileWrapperConstructor().newInstance(getConnectionWrapper(useKerberos),
                uri);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
                | InvocationTargetException | SecurityException e) {
            LOGGER.error("Failed to create MapR-Fs remote file: " + e.getMessage(), e);
            return null;
        }
    }

    public synchronized static MaprFsConnectionWrapper getConnectionWrapper(final boolean useKerberos) {
        if (maprFsConnection == null) {
            try {
                maprFsConnection = (MaprFsConnectionWrapper) getConnectionWrapperConstructor().newInstance(useKerberos);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException | SecurityException | ClassNotFoundException | IOException e) {
                LOGGER.error("Failed to create MapR-FS connection: " + e.getMessage(), e);
                return null;
            }
        }
        return maprFsConnection;
    }
}

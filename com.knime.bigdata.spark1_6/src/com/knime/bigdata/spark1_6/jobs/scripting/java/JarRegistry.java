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
 *   Created on Feb 5, 2016 by bjoern
 */
package com.knime.bigdata.spark1_6.jobs.scripting.java;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;

import com.knime.bigdata.spark.core.job.SparkClass;

/**
 * This class can be used to handle the loading of jars into a {@link SparkContext}, and to track which jars are already
 * loaded. There must only be one instance of this class per {@link SparkContext}. Instances of this class are
 * thread-safe, i.e. they can be safely used by concurrent jobs within the same {@link SparkContext}.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
@SparkClass
public class JarRegistry {

    private final static Logger LOGGER = Logger.getLogger(JarRegistry.class.getName());

    private static JarRegistry perContextSingleton;

    private final SparkContext m_context;

    private final HashSet<String> m_loadedJars;

    /**
     * @param context
     */
    protected JarRegistry(final SparkContext context) {
        this.m_context = context;
        this.m_loadedJars = new HashSet<String>();
    }

    /**
     * Ensures that a given jar file is in the classpath of the Spark driver and all executors.
     *
     * @param jarFilesToLoad A list of local filesystem paths that designate the jar files to load.
     * @throws MalformedURLException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     */
    public synchronized void ensureJarsAreLoaded(final List<File> jarFilesToLoad) throws NoSuchMethodException,
        IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
        // This should be the jobserver's own ContextURLClassLoader, which is a subclass of java.net.URLClassLoader
        // Unfortunately, ContextURLClassLoader is not part of the jobserver API (which we are building against), therefore
        // we can only cast to java.net.URLClassLoader.
        URLClassLoader contextClassLoader = (URLClassLoader)this.getClass().getClassLoader();

        for (File jarFileToLoad : jarFilesToLoad) {
            final String jarPath = jarFileToLoad.getAbsolutePath();

            if (!m_loadedJars.contains(jarPath)) {
                LOGGER.log(Level.INFO,
                    String.format("Loading jar file into Spark driver and workers: %s", jarFileToLoad.getName()));
                m_context.addJar(jarPath);
                addToClassLoader(new URL("file:" + jarPath), contextClassLoader);
                m_loadedJars.add(jarPath);
            }
        }
    }

    private void addToClassLoader(final URL jarFileURL, final URLClassLoader classLoader)
        throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        // URLClassLoader.addURL() is a protected method, hence we must invoke it via reflection.
        Method addURLMethod = findMethod(classLoader.getClass(), "addURL", URL.class);
        addURLMethod.setAccessible(true);
        addURLMethod.invoke(classLoader, jarFileURL);
    }

    private Method findMethod(final Class<?> clazz, final String methodName, final Class<?>... parameterTypes)
        throws NoSuchMethodException {

        Class<?> currClass = clazz;

        while (currClass != null) {
            try {
                return currClass.getDeclaredMethod(methodName, parameterTypes);
            } catch (NoSuchMethodException e) {
                currClass = clazz.getSuperclass();
            }
        }

        throw new NoSuchMethodException("Could not find method: " + methodName);
    }

    /**
     *
     * @param context A Spark context
     * @return A JarRegistry instance specific to the given Spark context.
     */
    public static synchronized JarRegistry getInstance(final SparkContext context) {
        if (perContextSingleton == null) {
            perContextSingleton = new JarRegistry(context);
        }

        if (perContextSingleton.m_context != context) {
            throw new RuntimeException(
                "Trying to use existing JarRegistry with a Spark context other than the one it was created from. This is a bug.");
        } else {
            return perContextSingleton;
        }
    }
}

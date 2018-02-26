package org.knime.bigdata.spark.local.wrapper;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.core.runtime.FileLocator;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

public class LocalSparkWrapperFactory {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(LocalSparkWrapperFactory.class);

	public static final String LOG4J_PACKAGE_NAME = "org.apache.log4j";

	/**
	 * Creates a {@link LocalSparkWrapper} that lives in its own class loader
	 * hierarchy. The returned object can be used to create/destroy a Spark
	 * context in local mode and run jobs on it.
	 * 
	 * @param jobJar
	 *            The KNIME job jar.
	 * @param extraJars
	 *            Additional jars that should be on the classpath of the custom
	 *            class loader hierarchy.
	 * @return a {@link LocalSparkWrapper}, which is ready to create a Spark
	 *         context in local mode.
	 */
	public static LocalSparkWrapper createWrapper(final File jobJar, final File[] extraJars) {

		try {
			final ClassLoader sparkClassLoader = createSparkClassLoader(jobJar, extraJars);
			return (LocalSparkWrapper) sparkClassLoader.loadClass(LocalSparkWrapperImpl.class.getName()).newInstance();

		} catch (IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			LOGGER.error(e);
		}
		return null;
	}

	private static ClassLoader createSparkClassLoader(final File jobJar, final File[] extraJars) throws IOException {
		final Bundle hadoopBundle = FrameworkUtil.getBundle(Configuration.class);
		final Bundle scalaLibraryBundle = FrameworkUtil.getBundle(scala.Boolean.class);
		final Bundle scalaReflectBundle = FrameworkUtil.getBundle(scala.reflect.api.Annotations.class);
		final Bundle scalaCompilerBundle = FrameworkUtil.getBundle(scala.tools.nsc.Main.class);
		final Bundle scalaPBundle = FrameworkUtil.getBundle(scala.tools.scalap.Main.class);
		final Bundle scalaXmlBundle = FrameworkUtil.getBundle(scala.xml.Document.class);
		
		// put io.nettyo on the package blacklist in the bundleDelegatingLoader,
		// because the hadoop bundle has this package on its classpath but in a version
		// that is older than the one in Spark. Otherwise we get NoSuchMethod exceptions.
		final ClassLoader bundleDelegatingLoader = new MultiBundleDelegatingClassloader(new String[] { "io.netty" },
				scalaLibraryBundle,
				scalaReflectBundle,
				scalaCompilerBundle,
				scalaPBundle,
				scalaXmlBundle,
				hadoopBundle);
		
		
		return new URLClassLoader(getJars(jobJar, extraJars), bundleDelegatingLoader) {
			public Class<?> loadClass(String name) throws ClassNotFoundException {
				// we need to intercept loading of the LocalSparkWrapper class
				// because
				// otherwise we cannot assign the LocalSparkWrapperImpl instance
				// to a variable of type LocalSparkWrapper
				// we also need to intercept class loading of log4j because this
				// will make Spark
				// use KNIME's already configured log4j logging system
				if (name.equals(LocalSparkWrapper.class.getName())) {
					return LocalSparkWrapperFactory.class.getClassLoader().loadClass(name);
				} else {
					// this tries to load classes from the urls and its parent
					// classloaders
					return super.loadClass(name);
				}
			}
		};
	}

	private static URL[] getJars(final File jobJar, final File[] extraJars) throws IOException {
		final File sparkJarDir = new File(FileLocator.getBundleFile(FrameworkUtil.getBundle(LocalSparkWrapperFactory.class)),
				"/libs");

		final List<URL> jarUrls = new LinkedList<>();
		// add job jar
		jarUrls.add(jobJar.toURI().toURL());
		
		// add the extra jars
		for(File extraJar : extraJars) {
			jarUrls.add(extraJar.toURI().toURL());
		}

		// add the Apache Spark jars
		for (File jarFile : sparkJarDir.listFiles()) {
			jarUrls.add(jarFile.toURI().toURL());
		}
		return jarUrls.toArray(new URL[jarUrls.size()]);
	}
}

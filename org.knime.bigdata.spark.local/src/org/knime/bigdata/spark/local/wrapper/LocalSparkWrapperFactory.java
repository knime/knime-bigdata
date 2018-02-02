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

	public static LocalSparkWrapper createWrapper(final File jobJar) {

		try {
			final ClassLoader sparkClassLoader = createSparkClassLoader(jobJar);
			return (LocalSparkWrapper) sparkClassLoader.loadClass(LocalSparkWrapperImpl.class.getName()).newInstance();

		} catch (IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			LOGGER.error(e);
		}
		return null;
	}

	private static ClassLoader createSparkClassLoader(final File jobJar) throws IOException {
		final Bundle hadoopBundle = FrameworkUtil.getBundle(Configuration.class);
		final Bundle scalaLibraryBundle = FrameworkUtil.getBundle(scala.Boolean.class);
		final Bundle scalaReflectBundle = FrameworkUtil.getBundle(scala.reflect.api.Annotations.class);
		final Bundle scalaCompilerBundle = FrameworkUtil.getBundle(scala.tools.nsc.Main.class);
		final Bundle scalaPBundle = FrameworkUtil.getBundle(scala.tools.scalap.Main.class);
		final Bundle scalaXmlBundle =FrameworkUtil.getBundle(scala.xml.Document.class);
		
		final ClassLoader bundleDelegatingLoader = new MultiBundleDelegatingClassloader(scalaLibraryBundle,
				scalaReflectBundle,
				scalaCompilerBundle,
				scalaPBundle,
				scalaXmlBundle,
				hadoopBundle);
		
//		final Bundle thisBundle = FrameworkUtil.getBundle(Configuration.class);
		
		return new URLClassLoader(getJars(jobJar), bundleDelegatingLoader) {
			public Class<?> loadClass(String name) throws ClassNotFoundException {
				// we need to intercept loading of the LocalSparkWrapper class
				// because
				// otherwise we cannot assign the LocalSparkWrapperImpl instance
				// to a variable of type LocalSparkWrapper
				// we also need to intercept class loading of log4j because this
				// will make Spark
				// use KNIME's already configured log4j logging system
				if (name.equals(LocalSparkWrapper.class.getName())) {
//						|| name.startsWith(LOG4J_PACKAGE_NAME)) {
					return LocalSparkWrapperFactory.class.getClassLoader().loadClass(name);
				} else {
					// this tries to load classes from the urls and its parent
					// classloaders
					return super.loadClass(name);
				}
			}
		};
	}

	private static URL[] getJars(final File jobJar) throws IOException {
		final File jarDir = new File(FileLocator.getBundleFile(FrameworkUtil.getBundle(LocalSparkWrapperFactory.class)),
				"/libs");

		final List<URL> jarUrls = new LinkedList<>();
		jarUrls.add(jobJar.toURI().toURL());

		for (File jarFile : jarDir.listFiles()) {
			jarUrls.add(jarFile.toURI().toURL());
		}
		return jarUrls.toArray(new URL[jarUrls.size()]);
	}
}

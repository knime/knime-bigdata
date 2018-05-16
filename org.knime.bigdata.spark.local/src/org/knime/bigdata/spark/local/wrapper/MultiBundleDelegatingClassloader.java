package org.knime.bigdata.spark.local.wrapper;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;

import org.osgi.framework.Bundle;

/**
 * Provides a classloader that tries to load a class from a given array of OSGI
 * {@link Bundle}s. The {@link #findClass(String)} method has been overridden to
 * try to load a given class from the bundles in the order they appear in the
 * array. The first bundle that successfully loads the class wins.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 *
 */
public class MultiBundleDelegatingClassloader extends ClassLoader {

	private final Bundle[] m_bundles;
	
	private final String[] m_pkgBlacklist;

	/**
	 * Creates a new instance.
	 * 
	 * @param pkgBlacklist
	 *            A package blacklist that is consulted before classloading.
	 *            This classloader only loads classes from those packages that
	 *            are not on the blacklist.
	 * @param bundles
	 *            An array of bundles that classes are to be loaded from.
	 */
	public MultiBundleDelegatingClassloader(final String[] pkgBlacklist, Bundle... bundles) {
		m_bundles = bundles;
		m_pkgBlacklist = pkgBlacklist;
	}

	/**
	 * Tries to find the given class from the underlying bundles in the order
	 * they appear in the underlying bundle array. The first bundle that
	 * successfully loads the class wins.
	 */
	@Override
    protected Class<?> findClass(final String name) throws ClassNotFoundException {
		Class<?> clazz = null;
		
		boolean onBlacklist = false;
		
		for(String blacklistedPkg : m_pkgBlacklist) {
			if (name.startsWith(blacklistedPkg)) {
				onBlacklist = true;
				break;
			}
		}
		
		if (onBlacklist) {
			throw new ClassNotFoundException("Package is on blacklist of MultiBundleDelegatingClassloader");
		}
		
		for (int i = 0; i < m_bundles.length; i++) {
			try {
				clazz = m_bundles[i].loadClass(name);
				break;
			} catch (ClassNotFoundException e) {
				// do nothing for now
			}
		}

		if (clazz != null) {
			return clazz;
		} else {
			throw new ClassNotFoundException("MultiBundleDelegatingClassloader could not find class " + name);
		}
	}

	/**
	 * Tries to find the given resource from the underlying bundles in the order
	 * they appear in the underlying bundle array. The first bundle that
	 * successfully finds the resource wins.
	 */
	@Override
    protected URL findResource(final String name) {
		for (int i = 0; i < m_bundles.length; i++) {
			URL resource = m_bundles[i].getResource(name);
			if (resource != null) {
				return resource;
			}
		}
		return null;
	}

	/**
	 * Tries to find the given resources from the underlying bundles in the
	 * order they appear in the underlying bundle array. The first bundle that
	 * successfully finds the resources wins.
	 */
	@Override
    protected Enumeration<URL> findResources(final String name) throws IOException {
		for (int i = 0; i < m_bundles.length; i++) {
			Enumeration<URL> urls = m_bundles[i].getResources(name);
			if (urls != null) {
				return urls;
			}
		}

		return Collections.emptyEnumeration();
	}

	/**
	 * Loads the class with the given binary name. This implementation invokes
	 * {@link #findClass(String)}. If a class was found and the resolve
	 * parameter was true, the class will be resolved with
	 * {@link #resolveClass(Class)}.
	 */
	@Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
		Class<?> clazz = findClass(name);

		if (resolve) {
			resolveClass(clazz);
		}
		return clazz;
	}

	/**
	 * @return the underlying array of bundles to load classes from.
	 */
	public Bundle[] getBundles() {
		return m_bundles;
	}
}

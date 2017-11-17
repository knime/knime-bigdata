package com.knime.tpbuilder

import scala.collection.mutable.Map
import scala.collection.mutable.Set

import com.knime.tpbuilder.TPConfigReader.TPConfig

/**
 * Checks that all artifacts to be bundled have a license.
 */
object LicenseChecker {
  
  
  def checkLicenses(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig) = {
    
    var missingLicenses = false
    
    for (art <- depGraph.keys) {
      if (art.bundle.get.licenses.isEmpty) {
        System.err.println(s"  MISSING LICENSE: ${art.mvnCoordinate} (bundle: ${art.bundle.get.bundleSymbolicName} version: ${art.bundle.get.bundleVersion.toString})")
        missingLicenses = true
      } else {
        println(s"${art.mvnCoordinate}|${art.bundle.get.licenses.map(_.toString).mkString("|")}")
      }
    }
    
    if (missingLicenses) {
      System.err.println(s"Some maven artifacts did contain license information in their maven metadata (see logging above).")
      System.err.println(s"Please manually specify license information for these artifacts (see licenses entry in the YAML config)")
      System.exit(1)
    }
  }
}  

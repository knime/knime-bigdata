package com.knime.tpbuilder

import scala.collection.mutable.Map
import scala.collection.mutable.Set

import com.knime.tpbuilder.TPConfigReader.TPConfig

/**
 * Checks that all artifacts to be bundled have a license.
 */
object LicenseChecker {
  
  
  def checkLicenses(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig) = {
    
    var fail = false
    
    for (art <- depGraph.keys) {
      if (art.bundle.get.licenses.isEmpty) {
        System.out.flush()
        System.err.println(s"  MISSING LICENSE: ${art.mvnCoordinate} (bundle: ${art.bundle.get.bundleSymbolicName} version: ${art.bundle.get.bundleVersion.toString})")
        fail = true
      } else {
        println(s"${art.mvnCoordinate}|${art.bundle.get.licenses.map(_.toString).mkString("|")}")
        if (art.bundle.get.licenses.size > 1) {
          System.out.flush()
          System.err.println(s"  ARTIFACT WITH MULTIPLE-LICENSES FOUND")
          fail = true
        }
      }
      // TODO: check for presence of license name and URL 
    }
    
    if (fail) {
      System.out.flush()
      System.err.println(s"Some maven artifacts contained no licenses or multiple licenses in their maven metadata (see logging above).")
      System.err.println(s"Please specify definitive license information for these artifacts in the YAML config (licenses section)")
      System.exit(1)
    }
  }
}  

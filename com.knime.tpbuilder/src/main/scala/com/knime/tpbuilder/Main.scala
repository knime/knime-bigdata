package com.knime.tpbuilder

import java.io.File

import scala.collection.mutable.HashMap
import scala.collection.mutable.Set

import org.apache.commons.io.FileUtils

import com.knime.tpbuilder.aether.DepGraphBuilder
import com.knime.tpbuilder.aether.DepGraphResolver
import com.knime.tpbuilder.osgi.Bundler
import com.knime.tpbuilder.osgi.P2SiteMaker

object Main extends App {

  // this is the default location for plugins that the maven plugin
  // org.eclipse.tycho.extras:tycho-p2-extras-plugin:1.0.0:publish-features-and-bundles
  // expects when generating an update site
  require(args.size > 0, s"You need to specify a target platform config (YAML) as first parameter")
  require(new File(args(0)).canRead(), s"Target platforn config ${args(1)} is not readable")
  
  val pluginDir = new File(new File(new File("target"), "source"), "plugins")
  FileUtils.deleteDirectory(pluginDir)
  FileUtils.forceMkdir(pluginDir)
    
  val config = TPConfigReader.read(args(0))
  val depGraph = HashMap[Artifact, Set[Artifact]]()
  
  for (artGroup <- config.artifactGroups) {
    println(s"=== Artifact group ${artGroup.name}: Collecting dependencies ===")
    DepGraphBuilder.makeDepGraph(artGroup, depGraph, config)
    println(s"Resolution group ${artGroup.name}: Dependency graph has %d artifacts now".format(depGraph.size))
    
    println()
  }
  println()
      
  println("=== Removing blacklisted maven artifacts from dependency graph ===")
  DepGraphCleaner.removeBlacklistedMavenArtifacts(depGraph, config)
  println("Dependency graph has %d artifacts now".format(depGraph.size))
  println()
  
  println("=== Removing artifacts from dependency graph that have the same major and minor version ===")
  DepGraphCleaner.removeDuplicateArtifacts(depGraph, config)
  println("Got dependency graph with %d artifacts".format(depGraph.size))
  println()
  
  println("=== Removing orphaned artifacts (i.e. those that nothing depends on) from dependency graph ===")
  DepGraphCleaner.removeOrphanedArtifacts(depGraph, config)
  println("Got dependency graph with %d artifacts".format(depGraph.size))
  println()
  
  println("=== Printing dependency graph ===")
  DepGraphPrinter.print(depGraph, config)
  println()
  
  println("=== Resolving all artifacts from dependency graph ===")
  DepGraphResolver.resolveDepGraph(depGraph, config)
  println()
  
  println("=== Checking licenses of maven artifacts ===")
  LicenseChecker.checkLicenses(depGraph, config)
  println()

  
  println("=== Merging artifacts by group ===")
  Merger.merge(depGraph, config)
  println("Got merged dependency graph with %d artifacts".format(depGraph.size))
  println()
  
  
  println("Creating bundles")
  Bundler.createBundles(depGraph, pluginDir, config)
  
  println("Creating P2 update site (via maven-invoker)")
  P2SiteMaker.createSite()
  
  FileUtils.deleteDirectory(pluginDir)
}

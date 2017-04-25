package com.knime.tpbuilder

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.collection.mutable.Seq
import org.osgi.framework.Version
import scala.collection.mutable.HashSet
import com.knime.tpbuilder.TPConfigReader.TPConfig
import java.util.regex.Pattern
import org.apache.maven.shared.osgi.DefaultMaven2OsgiConverter
import com.knime.tpbuilder.TPConfigReader.TPArtifactGroup

object DepGraphCleaner {
  
  private val maven2Osgi = new DefaultMaven2OsgiConverter()

  def removeBlacklistedMavenArtifacts(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig): Unit = {
    val patterns = config.mavenBlacklist.map(Pattern.compile)

    val toDelete = depGraph.keySet.filter(TPConfig.isMavenArtifactBlacklisted(config))
    
    toDelete.foreach(art => println(s"  Removing blacklisted maven artifact ${art.mvnCoordinate}"))
    
    depGraph --= toDelete
  }

  def removeDuplicateArtifacts(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig): Unit = {
    val removals = computeDuplicateRemovals(depGraph.keys, config)

    depGraph --= removals.keys
    
    val newDepGraph = Map[Artifact, Set[Artifact]]()
    
    for(art <- depGraph.keys) {
      val deps = depGraph(art)
      newDepGraph += art -> deps.map(artDep => removals.getOrElse(artDep, artDep))
    }
    
    depGraph.clear()
    depGraph ++= newDepGraph.iterator
    
    // we don't have to fix the dependencies on the artifacts that were removed
    // because all dependencies in the resulting bundles will be ranges of the form
    // [X.Y.0, X.Y+1.0).
  }

  private def computeDuplicateRemovals(artifacts: Iterable[Artifact], config: TPConfig): Map[Artifact, Artifact] = {
    // key is (groupId, artifactId, major.minor)
    val lookupMap = HashMap[String, Set[Artifact]]()
    
    // for conversion of maven version strings into osgi version strings
    

    for (art <- artifacts) {
      val key = s"${art.group}:${art.artifact}:${art.classifier.getOrElse("")}:${cropMicroVersion(toBundleVersion(art.version))}"
      lookupMap.getOrElseUpdate(key, HashSet[Artifact]()).add(art)
    }

    val removals = HashMap[Artifact, Artifact]()

    for (maybeDups <- lookupMap.values) {
      if (maybeDups.size > 1) {
        // sort ascending by version
        val sorted = maybeDups.toList.sortWith((l: Artifact, r: Artifact) => toBundleVersion(l.version).compareTo(toBundleVersion(r.version)) > 0)
        val newRemovals = sorted.tail.filterNot(TPConfig.isDuplicateRemovalBlacklisted(config))
        for (removal <- newRemovals) {
          println(s"Removing ${removal.mvnCoordinate} in favor of ${sorted.head.mvnCoordinate}")
          removals += removal -> sorted.head
        }
      }
    }

    removals
  }

  private def cropMicroVersion(osgiVersion: Version): String = {
    s"${osgiVersion.getMajor}.${osgiVersion.getMinor}"
  }
  
  private def toBundleVersion(mvnVersion: String): Version = {
    Version.parseVersion(maven2Osgi.getVersion(mvnVersion).replaceAll("[^A-Za-z0-9.]", ""))
  }

  def removeOrphanedArtifacts(depGraph: Map[Artifact, Set[Artifact]], config: TPConfig) = {
      val rootArts = config.artifactGroups.map(TPArtifactGroup.toArtifactSet).flatten(g => g).toSet
      
      val nonOrphans = HashSet[Artifact]()
      rootArts.foreach(addReachableArtifacts(nonOrphans, depGraph))
      
      val orphans = depGraph.keySet -- nonOrphans
      for (orphan <- orphans) {
        println(s"  Removing orphaned artifact ${orphan.mvnCoordinate}")
      }
      
      depGraph --= orphans 
  }
  
  private def addReachableArtifacts(reachableArts: Set[Artifact], depGraph: Map[Artifact, Set[Artifact]])(root: Artifact): Unit = {
    reachableArts += root
    depGraph.getOrElse(root, Set()).foreach(addReachableArtifacts(reachableArts, depGraph))
  }
}
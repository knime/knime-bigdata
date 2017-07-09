package com.knime.tpbuilder.aether

import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.collection.immutable.{ Set => ImmutableSet }
import com.knime.tpbuilder.TPConfigReader.TPConfig
import collection.JavaConverters._
import org.apache.maven.repository.internal.MavenRepositorySystemUtils
import org.eclipse.aether.DefaultRepositorySystemSession
import org.eclipse.aether.collection.DependencySelector
import org.eclipse.aether.graph.Dependency
import org.eclipse.aether.collection.DependencyCollectionContext
import org.eclipse.aether.artifact.DefaultArtifact
import org.eclipse.aether.artifact.{ Artifact => AetherArtifact }
import org.eclipse.aether.collection.CollectRequest
import org.eclipse.aether.collection.DependencyTraverser
import com.knime.tpbuilder.TPConfigReader.TPMavenOverride
import org.eclipse.aether.util.graph.selector.ExclusionDependencySelector
import org.eclipse.aether.util.graph.selector.AndDependencySelector
import org.eclipse.aether.graph.Exclusion
import com.knime.tpbuilder.Artifact
import org.eclipse.aether.resolution.ArtifactDescriptorRequest
import java.util.LinkedList
import scala.collection.mutable.Buffer
import org.eclipse.aether.util.graph.traverser.FatArtifactTraverser
import org.eclipse.aether.util.graph.traverser.AndDependencyTraverser
import scala.collection.mutable.HashMap
import scala.reflect._
import com.knime.tpbuilder.TPConfigReader.TPArtifactGroup
import com.knime.tpbuilder.TPConfigReader.TPMultiArtifact
import scala.collection.mutable.Queue

object DepGraphBuilder {

  private def newRepoSession(root: Dependency): DefaultRepositorySystemSession = {
    val session = AetherUtils.repoSession

    //    println("Eclusions: " + root.getExclusions.asScala.map(_.toString).mkString(", "))
    val exclusionSelector = new ExclusionDependencySelector(root.getExclusions)
    session.setDependencySelector(new AndDependencySelector(exclusionSelector, new DependencySelector() {
      def selectDependency(dependency: Dependency): Boolean = {
        if (dependency.getScope().equals("test")) {
          return false
        }

        return true
      }

      def deriveChildSelector(context: DependencyCollectionContext): DependencySelector = {
        return this
      }
    }))

    val fatJarTraverser = new FatArtifactTraverser()
    session.setDependencyTraverser(new AndDependencyTraverser(fatJarTraverser, new DependencyTraverser() {
      def traverseDependency(dependency: Dependency): Boolean = {
        return dependency.equals(root)
      }

      def deriveChildTraverser(context: DependencyCollectionContext): DependencyTraverser = {
        return this
      }
    }))

    session
  }

  def makeDepGraph(artGroup: TPArtifactGroup, depGraph: Map[Artifact, Set[Artifact]], config: TPConfig): Unit = {
    // register maven artifact repositories from config
    AetherUtils.initDefaultRemoteArtifactRepos(config)

    for (multiArt <- artGroup.multiArtifacts) {
      for (art <- TPArtifactGroup.toArtifactSet(multiArt)) {
        AetherUtils.remoteArtifactRepos += (art.mvnCoordinate -> AetherUtils.defaultRemoteArtifactRepos)

        makeDepGraphTransitively(art, depGraph, multiArt.transitive.get, config)
      }
    }

    for (singleArt <- artGroup.singleArtifacts) {
      val art = TPArtifactGroup.toArtifact(singleArt)

      AetherUtils.remoteArtifactRepos += (art.mvnCoordinate -> AetherUtils.defaultRemoteArtifactRepos)

      makeDepGraphTransitively(art, depGraph, singleArt.transitive.get, config)
    }
  }

  private def makeDepGraphTransitively(art: Artifact, depGraph: Map[Artifact, Set[Artifact]], transitive: Boolean, config: TPConfig): Unit = {
    val subDepGraph = HashMap[Artifact, Set[Artifact]]()
    val dep = AetherUtils.artToDep(art)

    mkDepGraphBreadthFirst(config, subDepGraph, dep, transitive)
    mergeGraphs(subDepGraph, depGraph)
  }

  private def mergeGraphs(from: Map[Artifact, Set[Artifact]], into: Map[Artifact, Set[Artifact]]) = {
    for ((art, artDeps) <- from.iterator) {
      if (into.contains(art)) {
        val existingDeps = into(art)
        into.update(art, artDeps ++ existingDeps)
      } else {
        into += art -> artDeps
      }
    }
  }

  private def mkDepGraphBreadthFirst(config: TPConfig,
    depGraph: Map[Artifact, Set[Artifact]],
    root: Dependency,
    transitive: Boolean): Unit = {

    val queue = Queue[Dependency]()
    queue.enqueue(root)

    while (!queue.isEmpty) {
      val dep = queue.dequeue()
      val depArt = AetherUtils.depToArt(dep)
      
      //exclusions.getOrElseUpdate(depArt.mvnCoordinate, Set()) ++= dep.getExclusions.asScala

      if (TPConfig.isMavenArtifactBlacklisted(config)(depArt)) {
        require(TPMavenOverride.maybeOverride(config.requireBundleOverrides, depArt).isDefined ||
          TPMavenOverride.maybeOverride(config.importPackageOverrides, depArt).isDefined ||
          TPConfig.isMavenDependencyBlacklisted(config)(depArt),
          s"Artifact ${depArt.mvnCoordinate} is blacklisted but there is not matching entry under mavenDependencyBlacklist, " +
            "requireBundleOverrides, or importPackageOverrides. Please provide one so we can either discard the dependency or generate a Require-Bundle/" +
            "Import-Package clause for it.")
      } else if (!depGraph.contains(depArt)) {

        println(s"Analyzing dependencies of ${depArt.mvnCoordinate}")

        val nextLevelDeps = collectDirectDeps(dep, config)

        depGraph += (depArt -> (Set() ++= nextLevelDeps.map(AetherUtils.depToArt)))

        if (transitive) {
          for (nextLevelDep <- nextLevelDeps) {

            val exclusionDependencySelector = new ExclusionDependencySelector(dep.getExclusions)

            if (!exclusionDependencySelector.selectDependency(nextLevelDep)) {
              println(s"  Excluding dependency ${AetherUtils.depToArt(nextLevelDep).mvnCoordinate()} (reason: maven exclusion)")
            } else if (nextLevelDep.isOptional()) {
              println(s"  Excluding dependency ${AetherUtils.depToArt(nextLevelDep).mvnCoordinate()} (reason: marked as optional)")
            } else if (nextLevelDep.getScope.equals("provided")) {
              println(s"  Excluding dependency ${AetherUtils.depToArt(nextLevelDep).mvnCoordinate()} (reason: scoped as provided)")
            } else {
              queue.enqueue(nextLevelDep)
            }
          }
        }
      }
    }
  }

  private def collectDirectDeps(root: Dependency, config: TPConfig): Buffer[Dependency] = {

    val session = newRepoSession(root)

    val collectRequest = new CollectRequest()
    collectRequest.setRoot(root)
    for (remoteRepo <- AetherUtils.remoteArtifactRepos(AetherUtils.depToArt(root).mvnCoordinate)) {
      collectRequest.addRepository(remoteRepo)
    }

    // first collect dependencies
    val rootNode = AetherUtils.repoSys.collectDependencies(session, collectRequest).getRoot()

    val directDeps = rootNode.getChildren.asScala
      .map(_.getDependency)

      // filter out all maven dependencies blacklisted in TPConfig
      .filterNot(dep => {
        val art = AetherUtils.depToArt(dep)
        if (TPConfig.isMavenDependencyBlacklisted(config)(art)) {
          println(s"  Removing blacklisted maven artifact ${art.mvnCoordinate()} from dependency graph")
          true
        } else false
      })

      // apply maven dependency overrides (from TPConfig)
      .map(dep => {
        val art = AetherUtils.depToArt(dep)
        TPMavenOverride.maybeOverride(config.mavenDependencyOverrides, art) match {
          case Some(mvnCoord) => {
            println(s"  Overriding maven dependency on ${art.mvnCoordinate()} with ${mvnCoord}")
            // this inherits the exclusions during maven dependency overriding
            new Dependency(AetherUtils.artToAetherArt(Artifact.fromMvnCoordinate(mvnCoord)),
              dep.getScope,
              dep.getOptional,
              dep.getExclusions)
          }
          case None => dep
        }
      })

      // merge exclusions (this inherits the exclusions of the current dependency to its child dependencies)
      .map(dep => {
        if (root.getExclusions.isEmpty) {
          dep
        } else {
          new Dependency(dep.getArtifact,
            dep.getScope,
            dep.getOptional,
            (Set() ++ dep.getExclusions.asScala ++ root.getExclusions.asScala).asJava)
        }
      })
    registerRepositoriesForDependencies(root, directDeps)
    directDeps
  }

  private def registerRepositoriesForDependencies(root: Dependency, directDeps: Iterable[Dependency]) = {
    val rootDepRepos = AetherUtils.remoteArtifactRepos.get(AetherUtils.depToArt(root).mvnCoordinate) match {
      case Some(repoSet) => repoSet
      case None => AetherUtils.defaultRemoteArtifactRepos
    }

    val artDescriptorRequest = new ArtifactDescriptorRequest()
    artDescriptorRequest.setArtifact(root.getArtifact)
    artDescriptorRequest.setRepositories(new LinkedList(rootDepRepos.asJavaCollection));

    val rootArtDescriptor = AetherUtils.repoSys.readArtifactDescriptor(AetherUtils.repoSession, artDescriptorRequest)

    val depRepos = rootDepRepos ++ rootArtDescriptor.getRepositories.asScala
    for (dep <- directDeps) {
      AetherUtils.remoteArtifactRepos += (AetherUtils.depToArt(dep).mvnCoordinate -> depRepos)
    }
  }

}
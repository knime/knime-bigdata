package com.knime.tpbuilder.maven

import java.util.LinkedList

import scala.collection.JavaConverters.asJavaCollectionConverter
import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.maven.model.License
import org.apache.maven.model.Model
import org.apache.maven.model.building.DefaultModelBuilderFactory
import org.eclipse.aether.resolution.ArtifactDescriptorRequest
import org.eclipse.aether.resolution.ArtifactDescriptorResult
import org.eclipse.aether.artifact.{ Artifact => AetherArtifact }

import com.knime.tpbuilder.Artifact
import com.knime.tpbuilder.aether.AetherUtils

object MavenHelper {

  private val modelBuilder = new DefaultModelBuilderFactory().newInstance()

  def getPomModel(art: AetherArtifact): Model = {
    val repoList = new LinkedList(AetherUtils.remoteArtifactRepos(AetherUtils.aetherArtToArt(art).mvnCoordinate).asJavaCollection)
    val req = new ArtifactDescriptorRequest(art, repoList, null)
    val result = AetherUtils.artDescriptorReader.readArtifactDescriptor(AetherUtils.repoSession, req)
  		
    // this invokes the private method loadPom method on the DefaultArtifactDescriptReader to get the maven model
    new PrivMethCaller(AetherUtils.artDescriptorReader, "loadPom")(AetherUtils.repoSession, req, new ArtifactDescriptorResult(req))
      .asInstanceOf[Model]
  }
  
  
  class PrivMethCaller(obj: AnyRef, methodName: String) {
    def apply(anyArgs: Any*): Any = {
      
      val argSeq = anyArgs.map(_.asInstanceOf[AnyRef])
      val meth = obj.getClass.getDeclaredMethods
        .find(_.getName == methodName)
        .getOrElse(throw new IllegalArgumentException(s"Could not find method $methodName"))
      meth.setAccessible(true)
      meth.invoke(obj, argSeq: _*)
    }
  }
}
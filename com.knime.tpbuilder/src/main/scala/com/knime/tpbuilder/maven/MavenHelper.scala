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

  def getLicenses(art: AetherArtifact): Seq[License] = {

    val repoList = new LinkedList(AetherUtils.remoteArtifactRepos(AetherUtils.aetherArtToArt(art).mvnCoordinate).asJavaCollection)
      val req = new ArtifactDescriptorRequest(art, repoList, null)
      
  		
  		val result = AetherUtils.artDescriptorReader.readArtifactDescriptor(AetherUtils.repoSession, req)
  		
  		// this invokes the private method loadPom method on the DefaultArtifactDescriptReader to get the maven model
  		val model = new PrivMethodExposer(AetherUtils.artDescriptorReader)('loadPom)(AetherUtils.repoSession, req, new ArtifactDescriptorResult(req)).asInstanceOf[Model]
  		
  		model.getLicenses.asScala
  }

  
  
  class PrivMethodCaller(obj: AnyRef, methodName: String) {
    def apply(_args: Any*): Any = {
      val args = _args.map(_.asInstanceOf[AnyRef])
      val declaredMethods = obj.getClass.getDeclaredMethods
      val method = declaredMethods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException(s"Method $methodName not found"))
      method.setAccessible(true)
      method.invoke(obj, args: _*)
    }
  }

  class PrivMethodExposer(obj: AnyRef) {
    def apply(method: scala.Symbol): PrivMethodCaller = new PrivMethodCaller(obj, method.name)
  }
}
name := "sparklens"
// TBD: Should we change this and publish on our own? Upstream does not seem to be
// merging PRs.
organization := "com.qubole"

val sparkVersion = settingKey[String]("Spark version")

scalaVersion := "2.12.15"

crossScalaVersions := Seq("2.12.15", "2.13.13")

sparkVersion := "3.3.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"

libraryDependencies +=  "org.apache.hadoop" % "hadoop-client" % "2.7.4" % "provided"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.13" % "provided"

libraryDependencies += "org.apache.httpcomponents" % "httpmime" % "4.5.13" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % "test"
libraryDependencies += "org.junit.platform" % "junit-platform-engine" % "1.6.3" % "test"
libraryDependencies += "org.junit.platform" % "junit-platform-launcher" % "1.6.3" % "test"


testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-target:jvm-1.8")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

publishMavenStyle := true


licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")


pomExtra :=
  <url>https://github.com/qubole/sparklens</url>
  <scm>
    <url>git@github.com:qubole/sparklens.git</url>
    <connection>scm:git:git@github.com:qubole/sparklens.git</connection>
  </scm>
  <developers>
    <developer>
      <id>iamrohit</id>
      <name>Rohit Karlupia</name>
      <url>https://github.com/iamrohit</url>
    </developer>
    <developer>
      <id>beriaanirudh</id>
      <name>Anirudh Beria</name>
      <url>https://github.com/beriaanirudh</url>
    </developer>
    <developer>
      <id>mayurdb</id>
      <name>Mayur Bhosale</name>
      <url>https://github.com/mayurdb</url>
    </developer>
  </developers>

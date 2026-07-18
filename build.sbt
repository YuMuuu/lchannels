lazy val commonSettings = Seq(
  version := "0.0.3",
  scalaVersion := "2.13.18",
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-unchecked",
    "-feature",
    "-Wunused:imports", // "-deprecation"
    "-Yrangepos",
    "-P:wartremover:only-warn-traverser:org.wartremover.warts.Unsafe"
  ),
  addCompilerPlugin(
    "org.scalameta" % "semanticdb-scalac" % "4.17.2" cross CrossVersion.full
  ),
  addCompilerPlugin(
    "org.wartremover" %% "wartremover" % "3.6.1" cross CrossVersion.full
  ),
  scalafixOnCompile := false,
  // ScalaDoc setup
  autoAPIMappings := true,
  Compile / doc / scalacOptions ++= Seq(
    "-no-link-warnings" // Workaround for ScalaDoc @throws links issues
  )
)

lazy val lchannels = (project in file("lchannels"))
  .settings(commonSettings: _*)
  .settings(
    name := "lchannels",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.6.21",
      "com.typesafe.akka" %% "akka-actor-typed" % "2.6.21",
      "com.typesafe.akka" %% "akka-remote" % "2.6.21"
    )
  )

lazy val examples = (project in file("examples"))
  .dependsOn(lchannels)
  .settings(commonSettings: _*)
  .settings(
    name := "lchannels-examples",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-simple" % "1.7.36",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.6"
    )
  )

lazy val benchmarks = (project in file("benchmarks"))
  .dependsOn(lchannels)
  .settings(commonSettings: _*)
  .settings(
    name := "lchannels-benchmarks"
    // Depending on the benchmark size and duration, you might want
    // to add the following options:
    //
    // fork := true, // Fork a JVM, running inside benchmarks/ dir
    // javaOptions ++= Seq("-Xms1024m", "-Xmx1024m") // Enlarge heap size
  )

lazy val root = (project in file("."))
  .aggregate(lchannels, examples, benchmarks)
  .settings(commonSettings: _*)
  .settings(
    publishArtifact := false,
    // Kludge to avoid building an empty .jar for the root project
    Keys.`package` := {
      (lchannels / Compile / Keys.`package`).value
      (examples / Compile / Keys.`package`).value
      (benchmarks / Compile / Keys.`package`).value
    }
  )

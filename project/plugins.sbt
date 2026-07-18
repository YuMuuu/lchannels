addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.16")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.29")

dependencyOverrides += "ch.epfl.scala" % "scalafix-interfaces" % "0.14.7"

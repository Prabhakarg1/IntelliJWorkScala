package com

class ScalaTestExample {

  def stringRevers(givenString: String): Unit = {
    try {
      println(s"Using revers function : $givenString.reverse")
      println("created new local branch")
      println("added just now"
      var builder: StringBuilder = StringBuilder.newBuilder;
      for (i <- givenString.size-1 to 0 by -1) {
        val chr = givenString.charAt(i.toInt)
        builder.append(chr)
        println(s" revers order using loops : $chr")
      }
      println(builder)
    }
    catch {
      case ex: StringIndexOutOfBoundsException => {
        println("Found array out of bound exception")
      }
    }
  }

}

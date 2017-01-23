package com.oda.brian

/**
  * Created by bhizzle on 1/22/17.
  */
object Aligner {
  def main(args: Array[String]): Unit = {
    // read in reference lines from file
    val fileLines = io.Source.fromFile("/nfs/home/blhill/code/github/SequenceAligner/sequence-aligner/src/test/resources/practice_W_1/ref_practice_W_1_chr_1.txt").getLines.toList
    // turn lines into a single string, skipping header line
    val reference_string = fileLines.slice(1, fileLines.length).mkString

    val test_string = new StringBuilder
    //test_string ++= "ACAACG"
    test_string ++= reference_string
    val BWT = BWTMatrix(test_string)
    BWT.foreach(println)
    getBWTMatrixFirstLastCols(BWT).foreach(println)
   
  }

  def BWTMatrix(T: StringBuilder) : Array[String] = {
    // create empty array of strings
    val arr_buff = collection.mutable.ArrayBuffer.empty[String]
    // if last character isn't a $, append one
    if (T(T.length - 1) != '$') {
      T ++= "$"
    }
    // get length of string
    val len = T.length
    // rotate the sequence by slicing at a location and appending it
    for (i <- 0 until len) {
      val s = T.toString.slice(i, len) + T.toString.slice(0, i)
      arr_buff += s
    }
    // sort the string and return
    arr_buff.toArray.sorted
  }

  def getBWTMatrixFirstLastCols(BWTMatrix: Array[String]) : Array[(Char, Char)] = {
    // return a tuple of the first character and the last character of each string
    BWTMatrix.map(s => (s(0), s(s.length -1 )))
  }
}

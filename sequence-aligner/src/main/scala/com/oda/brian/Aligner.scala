package com.oda.brian

import util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.{BufferedWriter, File, FileWriter}
/**
  * Created by blhill on 1/22/17.
  */
object Aligner {
  def main(args: Array[String]): Unit = {

    val runTest = 1
    val conf = new SparkConf().setAppName("Aligner")
    val sc = new SparkContext(conf)

    // read in reference lines from file
    val ref_file = scala.io.Source.fromFile(args(0)).getLines.toList
    val read_file = scala.io.Source.fromFile(args(1)).getLines.toList
    //    val fileLines = io.Source.fromFile("/nfs/home/blhill/code/github/SequenceAligner/sequence-aligner/src/test/resources/practice_W_1/ref_practice_W_1_chr_1.txt").getLines.toList
    // turn lines into a single string, skipping header line
    val reference_string = ref_file.slice(1, ref_file.length).mkString
    val reads = read_file.slice(1, read_file.length).mkString.split(",")
    println("Num reads: " + reads.length)

    // parallelize the reads in spark
    val distReads = sc.parallelize(reads)
    //

    if (runTest == 1) {
      val test_string = new StringBuilder
      test_string ++= "ACAACG"

      val BWTMatrix_test = createBWTMatrix(test_string)
      val BWT_test = getBWTMatrixLastCol(BWTMatrix_test)
      val countMap_test = createCountMap(BWT_test)
      val occurrences_test = createOccurrences(BWT_test)
      val test_queries = List("ACC", "CAA", "ACG", "GAC")
      test_queries.foreach(x => println(EXACTMATCH(x, countMap_test, occurrences_test)))
      System.exit(0)
    }
    val test_string = new StringBuilder
    //test_string ++= "ACAACG"
    test_string ++= reference_string

    var BWT = ""
    // if we don't get a .bwt file as input, create the transform
    if (args.length < 3) {
      val BWTMatrix = createBWTMatrix(test_string)
      BWT = getBWTMatrixLastCol(BWTMatrix)
      writeBWT(BWT, args(0) + ".bwt")
    }
    // otherwise read from file
    else {
      BWT = readBWT(args(2))
    }

    //BWT.foreach(println)

    var countMap: Map[Char, Int] = Map()
    // if we don't get a .cntmap file as input, create the count map
    if (args.length < 4) {
      countMap = createCountMap(BWT)
      writeCountMap(countMap, args(0) + ".cntmap")
    }
    // otherwise read from file
    else {
      countMap = readCountMap(args(3))
    }

    var occurrences = List.empty[Int]
    // if we don't get a .occ file as input, create the occurrences list
    if (args.length < 5) {
      occurrences = createOccurrences(BWT)
      writeOccurrences(occurrences, args(0) + ".occ")
    }
    // else read from file
    else {
      occurrences = readOccurrences(args(4))
    }



    println("getting match ranges...")
    //reads.foreach(x => println(getMatchRange(x, last_col, count_arr, char_offset_map)))
    //val match_ranges = distReads.map(x => getMatchRange(x.slice(0, 6), last_col, count_arr, char_offset_map))

    println("getting sequence positions...")
    //val match_range = getMatchRange("ACG", first_last_cols._2, count_arr, char_offset_map)
    //val seq_positions = match_ranges.map(x => getSequencePosition(last_col, count_arr, char_offset_map, x))
    //seq_positions.collect.foreach(println)
    //println("Number of matched reads: " + seq_positions.collect.count(x => x > 0))
  }

  def createBWTMatrix(T: StringBuilder) : Array[String] = {
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

  def getBWTMatrixLastCol(BWTMatrix: Array[String]) : String = {
    // return a tuple of the first character and the last character of each string
    //val first_col = BWTMatrix.map(s => s(0))
    val last_col_chars = BWTMatrix.map(s => s(s.length - 1))
    val last_col_string = new StringBuilder
    for (i <- last_col_chars) {
      last_col_string ++= i.toString
    }
    return last_col_string.toString
  }

  def writeBWT(BWT: String, Filename: String): Unit = {
    val file = new File(Filename + ".bwt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(BWT)
    bw.close
  }

  def readBWT(Filename: String) : String = {
    val BWT = scala.io.Source.fromFile(Filename).getLines.mkString
    return BWT
  }

  def createCountMap(BWT: String) : Map[Char, Int] = {
    // CREATE MAP BETWEEN CHAR AND OFFSET
    // get counts of letters
    val num_A = BWT.count(c => c == 'A')
    val num_C = BWT.count(c => c == 'C')
    val num_G = BWT.count(c => c == 'G')
    val num_T = BWT.count(c => c == 'T')
    // use counts of letters to get offsets (1 for the $ char)
    val offset_A = 0
    val offset_C = offset_A + num_A
    val offset_G = offset_C + num_C
    val offset_T = offset_G + num_G

    // create map between character and offset
    val char_count_map = Map('A' -> offset_A, 'C' -> offset_C, 'G' -> offset_G, 'T' -> offset_T, 'E' -> (BWT.length-2), '$' -> 0)
    println("charoffsetmap: " + char_count_map)
    return char_count_map
  }

  def writeCountMap(countMap: Map[Char, Int], Filename: String): Unit = {
    val file = new File(Filename + ".cntmap")
    val bw = new BufferedWriter(new FileWriter(file))
    val map_strings = countMap.map(x => x._1 + "," + x._2)
    for (line <- map_strings) {
      bw.write(line + "\n")
    }
    bw.close
  }

  def readCountMap(Filename: String) : Map[Char, Int] = {
    val countMapPairs = scala.io.Source.fromFile(Filename).getLines.map(_.split(","))
    var countMap: Map[Char, Int] = Map()
    countMapPairs.foreach(x => countMap += (x(0)(0) -> x(1).toInt))
    return countMap
  }

  def createOccurrences(BWT: String): List[Int] = {
    // get array consisting of counts of that letter up until that location
    val occurrences = ArrayBuffer.empty[Int]

    for (i <- 0 until BWT.length) {
      // count number of instances of letter at index in last col up until that point
      occurrences += BWT.slice(0, i).count(c => c == BWT(i))
    }
    return occurrences.toList
  }

  def writeOccurrences(Occurrences: List[Int], Filename: String): Unit = {
    val file = new File(Filename + ".occ")
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- Occurrences) {
      bw.write(line + "\n")
    }
    bw.close
  }

  def readOccurrences(Filename: String) : List[Int] = {
    val occurrences = scala.io.Source.fromFile(Filename).getLines.toList.map(_.toInt)
    return occurrences
  }

  def EXACTMATCH(query: String, countMap: Map[Char, Int], occurrence: List[Int]): (Int, Int) = {
    val next_char = Map('A' -> 'C', 'C' -> 'G', 'G' -> 'T', 'T' -> 'E')
    var c = query(query.length - 1)
    var start_ptr = countMap(c) + 1
    var end_ptr = countMap(next_char(c)) + 1
    var i = query.length - 2

    while (start_ptr < end_ptr && i >= 1) {
      c = query(i)
      println(c + ",(" + start_ptr + "," + end_ptr + "), " + i)
      start_ptr = countMap(c) + occurrence(start_ptr) + 1
      end_ptr = countMap(c) + occurrence(end_ptr) + 1
      i = i - 1
    }
    return (start_ptr, end_ptr)
  }

  def getMatchRange(query: String, last_col: List[Char], count_arr: ArrayBuffer[Int], char_offset_map: Map[Char, Int] ) : (Int, Int) = {



    // reverse the order of characters in the string
    val reverse_query = query.reverse

    var first_ptr = 0
    var last_ptr = last_col.length - 1
    var query_char = ' '
    breakable {
      // iterate through the reversed string, one char at a time
      for (i <- reverse_query.toString) {
        query_char = i
        // get index of first occurance of char from first_ptr
        val first_idx = last_col.indexOf(query_char, first_ptr)
        // if we get a negative index, the string doesn't exist
        if (first_idx < 0) {
          query_char = last_col(first_ptr)
        }
        // use this to get the count of that character
        val first_char_count = count_arr(first_idx)
        // use char count and char offset to get new ptr
        first_ptr = first_char_count + char_offset_map(query_char)

        // repeat process for last_ptr
        // get index of last occurance of char from last_ptr
        val last_idx = last_col.lastIndexOf(query_char, last_ptr)
        // if we get a negative index, the string doesn't exist
        if (last_idx < 0) {
          query_char = last_col(last_ptr)
        }
        // use this to get count of that character
        val last_char_count = count_arr(last_idx)
        // use char count and char offset to get new ptr
        last_ptr = last_char_count + char_offset_map(query_char)

        //println("first match:" + first_idx + " last match:" + last_idx)
        //println("(" + first_ptr + "," + last_ptr + ")")
        if (query_char != i) {
          println("Mismatch: expected " + i + " but found " + query_char)
        }
      }
      return (first_ptr, last_ptr)
    }
    return(-1, -1)
  }

  def getSequencePosition(last_col: List[Char], count_arr: ArrayBuffer[Int], char_offset_map: Map[Char, Int], ptr_pair: (Int, Int)) : Int = {
    val first_ptr = ptr_pair._1
    val last_ptr = ptr_pair._2
    // keep track of number of steps to beginning of string
    var walk_count = 0
    // if difference is zero we have a match
    if (last_ptr - first_ptr == 0) {
      println("we found a match!")
      var final_ptr = last_ptr
      if (final_ptr < 0) {
        return -1
      }
      while (last_col(final_ptr) != '$') {
        val walk_char_count = count_arr(final_ptr)
        final_ptr = walk_char_count + char_offset_map(last_col(final_ptr))
        walk_count = walk_count + 1
      }
      println("Sequence Pos: " + walk_count)
    }
    return walk_count
  }
}

package com.oda.brian

import util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * Created by blhill on 1/22/17.
  */
object Aligner {
  def main(args: Array[String]): Unit = {

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
    //
    val test_string = new StringBuilder
    //test_string ++= "ACAACG"
    test_string ++= reference_string
    val BWT = BWTMatrix(test_string)
    //BWT.foreach(println)
    //getBWTMatrixFirstLastCols(BWT).foreach(println)
    val first_last_cols = getBWTMatrixFirstLastCols(BWT)

    val last_col = first_last_cols._2
    // CREATE MAP BETWEEN CHAR AND OFFSET
    // get counts of letters
    val num_A = last_col.count(c => c == 'A')
    val num_C = last_col.count(c => c == 'C')
    val num_G = last_col.count(c => c == 'G')
    val num_T = last_col.count(c => c == 'T')
    // use counts of letters to get offsets (1 for the $ char)
    val offset_A = 1
    val offset_C = offset_A + num_A
    val offset_G = offset_C + num_C
    val offset_T = offset_G + num_G

    // create map between character and offset
    val char_offset_map = Map('A' -> offset_A, 'C' -> offset_C, 'G' -> offset_G, 'T' -> offset_T)
    println("charoffsetmap: " + char_offset_map)

    // get array consisting of counts of that letter up until that location
    val count_arr = ArrayBuffer.empty[Int]

    for (i <- 0 until last_col.length) {
      // count number of instances of letter at index in last col up until that point
      count_arr += last_col.slice(0, i).count(c => c == last_col(i))
    }

    //reads.foreach(x => println(getMatchRange(x, last_col, count_arr, char_offset_map)))
    val match_ranges = distReads.map(x => getMatchRange(x.slice(0, 28), last_col, count_arr, char_offset_map))

    //val match_range = getMatchRange("ACG", first_last_cols._2, count_arr, char_offset_map)
    val seq_positions = match_ranges.map(x => getSequencePosition(last_col, count_arr, char_offset_map, x))
    seq_positions.collect.foreach(println)
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

  def getBWTMatrixFirstLastCols(BWTMatrix: Array[String]) : (List[Char], List[Char]) = {
    // return a tuple of the first character and the last character of each string
    val first_col = BWTMatrix.map(s => s(0))
    val last_col = BWTMatrix.map(s => s(s.length - 1))
    return (first_col.toList, last_col.toList)
  }

  def getMatchRange(query: String, last_col: List[Char], count_arr: ArrayBuffer[Int], char_offset_map: Map[Char, Int] ) : (Int, Int) = {



    // reverse the order of characters in the string
    val reverse_query = query.reverse

    var first_ptr = 0
    var last_ptr = last_col.length - 1
    breakable {
      // iterate through the reversed string, one char at a time
      for (i <- reverse_query.toString) {
        // get index of first occurance of char from first_ptr
        val first_idx = last_col.indexOf(i, first_ptr)
        // if we get a negative index, the string doesn't exist
        if (first_idx < 0) {
          break
        }
        // use this to get the count of that character
        val first_char_count = count_arr(first_idx)
        // use char count and char offset to get new ptr
        first_ptr = first_char_count + char_offset_map(i)

        // repeat process for last_ptr
        // get index of last occurance of char from last_ptr
        val last_idx = last_col.lastIndexOf(i, last_ptr)
        // if we get a negative index, the string doesn't exist
        if (last_idx < 0) {
          break
        }
        // use this to get count of that character
        val last_char_count = count_arr(last_idx)
        // use char count and char offset to get new ptr
        last_ptr = last_char_count + char_offset_map(i)

        //println("first match:" + first_idx + " last match:" + last_idx)
        //println("(" + first_ptr + "," + last_ptr + ")")
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

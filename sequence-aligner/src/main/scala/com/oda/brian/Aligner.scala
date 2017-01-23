package com.oda.brian

/**
  * Created by blhill on 1/22/17.
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

  def getBWTMatrixFirstLastCols(BWTMatrix: Array[String]) : (List[Char], List[Char]) = {
    // return a tuple of the first character and the last character of each string
    val first_col = BWTMatrix.map(s => s(0))
    val last_col = BWTMatrix.map(s => s(s.length - 1))
    return (first_col, last_col)
  }

  def getMatchRange(query: String, last_col: List[Char] ) : (Int, Int) = {
    // get counts of letters
    val num_A = last_col.count(c => c == 'A')
    val num_C = last_col.count(c => c == 'C')
    val num_G = last_col.count(c => c == 'G')
    val num_T = last_col.count(c => c == 'T')
    // use counts of letters to get offsets (1 for the $ char)
    val offset_A = 1 + num_A
    val offset_C = offset_A + num_C
    val offset_G = offset_C + num_G
    val offset_T = offset_G + num_T
    // create map between character and offset
    val char_offset_map = Map('A' -> offset_A, 'C' -> offset_C, 'G' -> offset_G, 'T' -> offset_T)

    // get array consisting of counts of that letter up until that location
    val count_arr = new collection.mutable.ArrayBuffer.empty[Int]
    for (i <- 0 until last_col.length) {
      // count number of occurances of letter at index in last col up until that point
      count_arr ++= last_col.slice(0, i).count(c => c == last_col(i))
    }
    println(count_arr)

    // reverse the order of characters in the string
    val reverse_query = query.reverse

    var first_ptr = 0
    var last_ptr = last_col.length - 1
    // iterate through the reversed string, one char at a time
    for (i <- reverse_query) {
      // get index of first occurance of char from first_ptr
      val first_idx = last_col.indexOf(i, first_ptr)
      // use this to get the count of that character
      val first_char_count = count_arr(first_idx)
      // use char count and char offset to get new ptr
      first_ptr = first_char_count + char_offset_map(i)
      println("First ptr:" + first_ptr)
      // repeat process for last_ptr
      // get index of last occurance of char from last_ptr
      val last_idx = last_col.lastIndexOf(i, last_ptr)
      // use this to get count of that character
      val last_char_count = count_arr(last_idx)
      // use char count and char offset to get new ptr
      last_ptr = last_char_count + char_offset_map(i)
      println("Last ptr:" + last_ptr)
    }

  }
}

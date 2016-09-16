package edu.uchicago.cs.dbp

import org.junit.Test
import org.junit.Assert._

class MergeStepTest {

  @Test
  def testConvertTM: Unit = {

    var tm = new TriangularMatrix(4)
    for (i <- 0 to 3) {
      for (j <- i to 3) {
        tm.set(i, j, i * 4 + j + 1)
      }
    }

    var ms = new MergeStep(0, 1)
    var converted = ms.convert(tm)
    assertEquals(3, converted.size)
    assertEquals(8, converted.get(0, 0))
    assertEquals(10, converted.get(0, 1))
    assertEquals(12, converted.get(0, 2))
    assertEquals(11, converted.get(1, 1))
    assertEquals(12, converted.get(1, 2))
    assertEquals(16, converted.get(2, 2))

    ms = new MergeStep(0, 2)
    converted = ms.convert(tm)
    assertEquals(3, converted.size)

    assertEquals(7, converted.get(0, 0))
    assertEquals(14, converted.get(0, 1))
    assertEquals(16, converted.get(0, 2))
    assertEquals(7, converted.get(1, 1))
    assertEquals(8, converted.get(1, 2))
    assertEquals(16, converted.get(2, 2))

    ms = new MergeStep(1, 3)
    converted = ms.convert(tm)
    assertEquals(3, converted.size)

    assertEquals(4, converted.get(0, 0))
    assertEquals(2, converted.get(0, 1))
    assertEquals(4, converted.get(0, 2))
    assertEquals(17, converted.get(1, 1))
    assertEquals(24, converted.get(1, 2))
    assertEquals(12, converted.get(2, 2))

  }

  @Test
  def testConvertPsize: Unit = {
    var psize = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    var ms = new MergeStep(3, 5)

    assertArrayEquals(Array(1, 2, 3, 10, 5, 7, 8, 9, 10), ms.convert(psize))
  }
}
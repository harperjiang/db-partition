package edu.uchicago.cs.dbp

import org.junit.Test
import org.junit.Assert._

class TriangularMatrixTest {

  @Test
  def testTranslate: Unit = {
    var tm = new TriangularMatrix(4)

    assertEquals(0, tm.translate(0, 0))
    assertEquals(1, tm.translate(0, 1))
    assertEquals(2, tm.translate(0, 2))
    assertEquals(3, tm.translate(0, 3))
    assertEquals(4, tm.translate(1, 1))
    assertEquals(5, tm.translate(1, 2))
    assertEquals(6, tm.translate(1, 3))
    assertEquals(7, tm.translate(2, 2))
    assertEquals(8, tm.translate(2, 3))
    assertEquals(9, tm.translate(3, 3))
  }

  @Test
  def testPrint: Unit = {
    var tm = new TriangularMatrix(4)
    
    tm.set(0, 0, 1)
    tm.set(0, 1, 2)
    tm.set(0, 2, 3)
    tm.set(0, 3, 4)
    tm.set(1, 1, 5)
    tm.set(1, 2, 6)
    tm.set(1, 3, 7)
    tm.set(2, 2, 8)
    tm.set(2, 3, 9)
    tm.set(3, 3, 10)
    
    tm.print()
  }
}
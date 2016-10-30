package edu.uchicago.cs.dbp

import org.junit.Test
import org.junit.Assert._

class DemoSaveableParams extends SaveableParams {
  var a = -1
  var b = 3d
  var c = 4
}

class SaveableParamsTest {

  @Test
  def testUse: Unit = {
    var demo = new DemoSaveableParams
    demo.a = 0
    demo.b = 0
    demo.c = 0

    demo.save

    demo.a = 5
    demo.b = 5
    demo.c = 5

    demo.save
    
    demo.a = 7
    demo.b = 6
    demo.c = 2
    
    demo.load
    
    assertEquals(5, demo.a)
    assertEquals(5, demo.b, 0.001)
    assertEquals(5, demo.c)
    
    demo.load

    assertEquals(0, demo.a)
    assertEquals(0, demo.b, 0.001)
    assertEquals(0, demo.c)
  }
}
package edu.uchicago.cs.dbp.online

import edu.uchicago.cs.dbp.online.leopard.LeopardParams

object ScoreFunc {
  
  def leopardScore: (Double, Int) => Double = {
    (neighbors: Double, size: Int) => { neighbors - LeopardParams.wSize * LeopardParams.eSize * Math.pow(size, LeopardParams.eSize - 1) / 2; }
  }
}
package edu.uchicago.cs.dbp.online.mlayer

import edu.uchicago.cs.dbp.SaveableParams

object MLayerParams {
  val instance = new MLayerParams

  def neighborThreshold = instance neighborThreshold
  def mergeThreshold = instance mergeThreshold
  def reassignRatio = instance reassignRatio

  def save = instance save
  def load = instance load
}

class MLayerParams extends SaveableParams {
  var neighborThreshold = 10d
  var mergeThreshold = 3d
  var reassignRatio = 0.5d;
}
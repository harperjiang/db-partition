package edu.uchicago.cs.dbp.online.mlayer

object MLayerParams {

  val map = new ThreadLocal[MLayerParams] {
    override def initialValue: MLayerParams = new MLayerParams
  }

  def get: MLayerParams = map.get

  def neighborThreshold = get neighborThreshold
  def mergeThreshold = get mergeThreshold
  def reassignRatio = get reassignRatio
}

class MLayerParams {
  var neighborThreshold = 10d
  var mergeThreshold = 3d
  var reassignRatio = 0.5d;
}
package edu.uchicago.cs.dbp.hadoop.stg2.itconvert

object ScalaTool extends App {
  
  var a = List(1,3,5,7);
  var head = 0;
  var prev = 0;
  a.foreach{value=>{
    prev = value;
    if(head ==0)
      head = prev;
  }};
  System.out.println(head);
  System.out.println(prev);
}
package gplume.scala.jdbc.pg

import java.sql.Date
import java.lang.{StringBuilder =>JStrBuilder}
import gplume.scala.jdbc.SQLAux.PaddingParam

/**
  * Created by Bowen Cai on 2/27/2016.
  */
object PgFastOps {

  def quoteTo(str: String)(implicit sb:JStrBuilder):JStrBuilder = {
    var _idx1 = str.indexOf(''')
    if (_idx1 < 0)
      sb.append(''').append(str).append(''')
    else {
      sb.append(''').append(str, 0, _idx1)
      val _strL = str.length
      while (_idx1 < _strL) {
        val ch = str.charAt(_idx1)
        if (ch == ''')
          sb.append(''').append(''')
        else
          sb.append(ch)
        _idx1 += 1
      }
      sb.append(''')
    }
  }

  implicit class PgInterpolation(val s: StringContext) extends AnyVal {

    def psql(params: Any*): PgFastOps = {
//      val sq = params.toSeq
//      new PgOperation(buildQuery(sq), sq)
      null.asInstanceOf[PgFastOps]
    }

  }

  def main(args: Array[String]): Unit = {
    val i = 5
    val d = 5.6
    val f = 5.5F
    val s = "shoot! \"p apa\" ' pil' 'pil '00"
    val ch = 'c'
    val date = new Date(System.currentTimeMillis())
    val nil = psql"SELECT $i and $d and $f and $s and $ch and $date"
    println(nil)
    println(quoteTo(s)(new JStrBuilder(128)))

  }
}
class PgFastOps(stmt:String) {

}

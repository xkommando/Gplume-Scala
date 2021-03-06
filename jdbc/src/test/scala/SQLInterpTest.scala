import com.zaxxer.hikari.HikariDataSource
import gplume.scala.jdbc._
import gplume.scala.jdbc.SQLAux._
import gplume.scala.jdbc.SQLOperation._
import org.junit.{Test, Before}

import scala.Seq
import scala.util.Try

/**
* Created by Bowen Cai on 12/27/2014.
*/
class SQLInterpTest {

  val ds = new HikariDataSource()

  @Before
  def t0: Unit ={
    ds.setDriverClassName("org.h2.Driver")
    ds.setJdbcUrl("jdbc:h2:mem:gscala_test")
    val con = ds.getConnection()
    con.createStatement().executeUpdate("""
CREATE TABLE data (
key VARCHAR(255) PRIMARY KEY,
value1 VARCHAR(1023), value2 VARCHAR(1023) )""")
    con.close()
  }


  @Test
  def t12: Unit = {
    val db = new DB(ds)
    db.newSession{implicit session =>
//      sql"insert into DATA(key,VALUE1, VALUE2)VALUES ('k1','v11','v12'),('k1,'v21','v22')".execute(session)
      sql"insert into DATA(key,VALUE1, VALUE2)VALUES (?,?,?)".batchInsert(
        Seq(Seq("kk1", "111","222"),
          Seq("kk2", "222","333"),
          Seq("kk3", "333", "444")
      ))
      val kk3 = "kk3"
      println(sql"SELECT COUNT(1) FROM `DATA` WHERE key=$kk3 ".first(colInt))
      println(sql"SELECT VALUE2 FROM `DATA` WHERE key=$kk3".first(colStr))
      println(sql"SELECT * FROM `DATA` WHERE key=$kk3".autoMap())
      val tp = (sql"SELECT * FROM `DATA` WHERE key=$kk3".product()).asInstanceOf[Tuple3[String, String, String]]
      println(tp)
      println(tp._3)
//      println(tp.productElement(1))
//      println(sql"SELECT COUNT(1) FROM `DATA` ".int(1))

    }
  }

  @Test
  def t1: Unit = {
    val p1 = 5
    val p2 = "data"
    val p3 = List(1)
    val so = sql"SELECT * FROM $p2 WHERE id = $p1 AND $p3"
//    println(so.stmt)
  }

  @Test
  def q: Unit ={
    val k1 = "key 111"
    val v1 = "value 111"
    implicit val session = new DBSession(ds.getConnection, false)
    val ins = sql"INSERT INTO `data` (key,value)VALUES($k1, $v1)".batchInsert(
      Seq(Seq("111","222"),
          Seq("222","333"),
          Seq("333", "444")
      )
    )

    val count = sql"SELECT COUNT (1) FROM `data`".first(colInt)
    println(count)
    val k1q = sql"SELECT value from data where key = $k1 OR key = '333'".first(colStr)
    println(k1q)
    val lsv = sql"SELECT value from data".array(colStr)
    println(lsv)
  }

  @Test
  def tnx: Unit = {
    val db = new DB(ds)
    val r = Try {
      db.transactional {implicit session=>
        val ins = sql"INSERT INTO `data` (key,value)VALUES(?, ?)".batchInsert(
          Seq(Seq("111", "222"),
            Seq("222", "333"),
            Seq("333", "444")
          )
        )
        db.transactional {implicit session =>
          val ins2 = sql"INSERT INTO `data` (key,value)VALUES(?, ?)".batchInsert(
            Seq(Seq("444", "555"),
              Seq("555", "666")
            )
          )
        }
        throw new RuntimeException
      }
    }
    db.newSession{ implicit session =>
      val count = sql"SELECT COUNT (1) FROM `data`".first(colInt)
      println(count)
    }
    db.execute("DELETE FROM `data`")
    db.newSession{ implicit session =>
      val count = sql"SELECT COUNT (1) FROM `data`".first(colInt)
      println(count)
    }
    println(r)
  }
}









package gplume.scala.jdbc

import java.math.MathContext
import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import com.sun.org.apache.xml.internal.utils.res.IntArrayWrapper
import gplume.scala.{Tuples, Tuple0}

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.collection.parallel.ParMap
import scala.collection.parallel.immutable.ParVector
import scala.collection.parallel.mutable.ParArray
import scala.reflect.ClassTag

/**
 * Created by Bowen Cai on 12/27/2014.
 */
object SQLOperation {

  def apply(value: String, parameters: Seq[Any] = Nil) = new SQLOperation(value, parameters)

  def unapply(op: SQLOperation): Option[(String, Seq[Any])] = Some((op.stmt, op.parameters))

  // single value collectors
  @inline val colBool = (rs: ResultSet) => rs.getBoolean(1)
  @inline val colByte = (rs: ResultSet) => rs.getByte(1)
  @inline val colShort = (rs: ResultSet) => rs.getShort(1)
  @inline val colInt = (rs: ResultSet) => rs.getInt(1)
  @inline val colLong = (rs: ResultSet) => rs.getLong(1)
  @inline val colFloat = (rs: ResultSet) => rs.getFloat(1)
  @inline val colDouble = (rs: ResultSet) => rs.getDouble(1)
  @inline val colBytes = (rs: ResultSet) => rs.getBytes(1)
  @inline val colStr = (rs: ResultSet) => rs.getString(1)
  @inline val colDate = (rs: ResultSet) => rs.getDate(1)
  @inline val colTime= (rs: ResultSet) => rs.getTime(1)
  @inline val colBigDecimal = (rs: ResultSet) => new BigDecimal(rs.getBigDecimal(1), MathContext.DECIMAL128)
  @inline val colObj = (rs: ResultSet) => rs.getObject(1)
  @inline val colBlob = (rs: ResultSet) => rs.getBlob(1)
  @inline val colClob = (rs: ResultSet) => rs.getClob(1)

  // no operation
  def NOP[A]: A=>Unit = (a: A)=>{}
  //  no return
  //  def NRET[A, B]: A=>B = (a:A)=>{null.asInstanceOf[B]}

  @inline
  def collectArray[A](rs: ResultSet, @inline extract: ResultSet => A): Array[A] = {
    if (rs.next()) {
      val head = extract(rs)
      val ab = Array.newBuilder[A](ClassTag(head.getClass))
      ab.sizeHint(64)
      ab += head
      while (rs.next())
        ab += extract(rs)
      ab.result()
    } else null.asInstanceOf[Array[A]]
  }

  @inline
  def collectList[A](rs: ResultSet, @inline extract: ResultSet => A): List[A] = {
    val ab = List.newBuilder[A]
    ab.sizeHint(32)
    while (rs.next())
      ab += extract(rs)
    ab.result()
  }

  @inline
  def collectVec[A](rs: ResultSet, @inline extract: ResultSet => A): Vector[A] = {
    val ab = Vector.newBuilder[A]
    ab.sizeHint(32)
    while (rs.next())
      ab += extract(rs)
    ab.result()
  }

  /**
   * collect result to a map
   * each row creates one key & value pair
   *
   * @param rs
   * @param extract
   * @tparam K
   * @tparam V
   * @return
   */
  @inline
  def collectMap[K,V](rs: ResultSet, @inline extract: ResultSet => (K,V)): Map[K,V] = {
    val mb = Map.newBuilder[K,V]
    mb.sizeHint(64)
    while (rs.next())
      mb += extract(rs)
    mb.result()
  }

  /**
   * collect result to a map of vectors
   * the keys are strings of names of the database columns,
   * the values are vectors of values from the first row of the resultSet
   *
   * @param rs
   * @return
   */
  @inline
  def collectToMap(rs: ResultSet): Map[String, Any] = {
    val md = rs.getMetaData
    val columnCount = md.getColumnCount
    if (columnCount <= 0 || !rs.next())
      return Map.empty[String, Any]
    val mb = Map.newBuilder[String, Any]
    mb.sizeHint(32)
//    (1 to columnCount).foreach(i=>{
//      mb += SQLAux.lookupColumnName(md, i) -> SQLAux.getResultSetValue(rs, i)
//    })
    var i = 1
    while (i <= columnCount) { // while loop is faster
      mb += SQLAux.lookupColumnName(md, i) -> SQLAux.getResultSetValue(rs, i)
      i += 1
    }
    mb.result()
  }

  /**
   * collect result to a map of vectors
   * the keys are strings of names of the database columns,
   * the values are vectors of values from each row of the resultSet
   *
   * @param rs
   * @return
   */
  def autoColGroup(rs: ResultSet): Map[String, Vector[Any]] = {
    val metaData = rs.getMetaData
    val columnCount = metaData.getColumnCount
    if (columnCount <= 0 || !rs.next())
      return Map.empty[String, Vector[Any]]

    val colRange = 1 to columnCount
    /**
     * find names
     * create vector builders for each column, and put the column name, together with value from first row to the builder
     */
    val grp = colRange.foldLeft(new VectorBuilder[(String, VectorBuilder[Any])])(
      (vec, colIdx) => vec += Tuple2(SQLAux.lookupColumnName(metaData, colIdx),
                            new VectorBuilder[Any] += SQLAux.getResultSetValue(rs, colIdx) )
    ).result()

    /**
     * extract values from rest rows
     */
    while (rs.next()) {
      for (i <- colRange) {
        grp(i - 1)._2 += SQLAux.getResultSetValue(rs, i)
      }
    }

    /**
     * make map
     */
    grp.foldLeft(Map.newBuilder[String, Vector[Any]])(
      (b, p) => b += p._1 -> p._2.result()
    ).result()
  }

  def collectToProduct(implicit rs: ResultSet): Product = {
    implicit val md = rs.getMetaData
    val columnCount = md.getColumnCount
    if (columnCount <= 0 || !rs.next())
      return Tuple0
    val mb = Array.newBuilder[AnyRef]
    mb.sizeHint(columnCount * 4 / 3 + 1)
    for (i <- 1 to columnCount)
      mb += SQLAux.getResultSetValue(rs, i)
    Tuples.toTuple(mb.result())
  }


}

class SQLOperation (val stmt: String, var parameters: Seq[Any] = Nil) {

  var queryTimeout: Int = 5

  import gplume.scala.jdbc.SQLOperation._

  def bind(params: Any*): SQLOperation = {
    parameters = Seq(params: _*)
    this
  }

  /**
   * base method
    *
    * @param prepare get preparedStatement from connection, e.g., generated keys.
   * @param process process the preparedStatement, e.g., execute, executeQuery, executeBatch,
   * @param session
   * @tparam A
   * @return
   */
  def exe[A](@inline prepare: Connection => PreparedStatement,
             @inline process: PreparedStatement => A)
            (implicit session: DBSession): A = {

    val ps = prepare(session.connection)
    SQLAux.bind(ps, parameters)
    val a = process(ps)
    ps.close()
    a
  }

  def batchExe[A](@inline prepare: Connection => PreparedStatement,
                  paramsList: Seq[Seq[Any]],
                  @inline process: PreparedStatement => A)(implicit session: DBSession): A = {
    val ps = prepare(session.connection)
    paramsList.foreach(t => {
      SQLAux.bind(ps, t)
      ps.addBatch()
    })
    val a = process(ps)
    ps.close()
    a
  }

  val getStmt = (con: Connection) => {
    val ps = con.prepareStatement(stmt)
    ps.setQueryTimeout(queryTimeout)
    ps
  }


  /**
   * batch execute sql statement, each statement is created with one seq of parameters.
    *
    * @param paramsList
   * @param session
   * @tparam A
   * @return
   */
  def batchExe[A](paramsList: => Seq[Seq[Any]])(implicit session: DBSession): Array[Int]
  = batchExe(getStmt, paramsList, process = ps => {
    val updateCounts = ps.executeBatch()
    session.checkWarnings(ps)
    updateCounts
  })(session)


  /**
   * execute this statement directly (no param binding).
    *
    * @param session
   * @return true if there is a resultSet or update count > 0
   */
  @inline
  def execute()(implicit session: DBSession)
  = exe(getStmt, process = ps => {
    val hasResultSet = ps.execute()
    session.checkWarnings(ps)
    hasResultSet || ps.getUpdateCount > 0
  })(session)

  val getStmtForInsert = (con: Connection) => {
    val ps = con.prepareStatement(stmt, Statement.RETURN_GENERATED_KEYS)
    ps.setQueryTimeout(queryTimeout)
    ps
  }


  /**
   * execute statement and returns generated keys.
    *
    * @param extract extract data from statement.getGeneratedKeys
   * @param session
   * @tparam A
   * @return
   */
  @inline
  def insert[A](@inline extract: ResultSet => A)(implicit session: DBSession): Option[A]
  = exe(getStmtForInsert, ps => {
    ps.execute()
    session.checkWarnings(ps)
    val rs = ps.getGeneratedKeys
    if (rs.next())
      Some(extract(rs))
    else
      None
  })

  def update(before: PreparedStatement => Unit)(implicit session: DBSession): Int
  = exe(getStmt, process = ps=>{
      before(ps)
      ps.executeUpdate()
    })


  /**
   * batch execute sql statement, each statement is created with one seq of parameters.
   *
   * @param paramsList
   * @param session
   * @return updateCounts
   */
  @inline
  def batchInsert(paramsList: => Seq[Seq[Any]])(implicit session: DBSession): Array[Int]
  = batchExe(getStmt,
    paramsList,
    ps => {
      val updateCounts = ps.executeBatch()
      session.checkWarnings(ps)
      updateCounts
    })


  /**
   *
   * batch execute sql statement, each statement is created with one seq of parameters.
   *
   * @param extract  extract data from statement.getGeneratedKeys
   * @param paramsList
   * @param session
   * @tparam A
   * @return data extracted from getGeneratedKeys
   */
  @inline
  def batchInsert[A](@inline extract: ResultSet => A,
                     paramsList: => Seq[Seq[Any]])(implicit session: DBSession): Array[A]
  = batchExe(getStmtForInsert,
    paramsList,
    ps => {
      ps.executeBatch()
      session.checkWarnings(ps)
      collectArray(ps.getGeneratedKeys, extract)
    })


  /**
   * base method for query.
    *
    * @param mapper map ResultSet to data A
   * @param before process PreparedStatement before executing the query, e.g., set statement parameters
   * @param session
   * @tparam A
   * @return
   */
  @inline
  def query[A](mapper: ResultSet => A,
               @inline before: PreparedStatement => Unit)
              (implicit session: DBSession): A
  = exe(getStmt, process = ps => {
    before(ps)
    val rs = ps.executeQuery()
    session.checkWarnings(ps)
    val ret = mapper(rs)
    rs.close()
    ret
  })


  /**
   * extract data from first row.
    *
    * @param extract
   * @param before process PreparedStatement before executing the query, e.g., set statement parameters
   * @param session
   * @tparam A
   * @return
   */
  @inline
  def first[A](@inline extract: ResultSet => A,
               @inline before: PreparedStatement => Unit = NOP[PreparedStatement])
              (implicit session: DBSession): Option[A]
  = query[Option[A]](mapper = rs => {
    if (rs.next()) Some(extract(rs)) else None
  }, before)(session)


  /**
   * collect results to an array.
   *
   * @param extract
   * @param before process PreparedStatement before executing the query, e.g., set statement parameters
   * @param session
   * @tparam A
   * @return
   */
  @inline
  def array[A](extract: ResultSet => A,
               before: PreparedStatement => Unit = NOP[PreparedStatement])
              (implicit session: DBSession): Array[A]
  = query[Array[A]](collectArray(_, extract), before)(session)


  /**
   * collect results to a list.
   *
   * @param extract
   * @param before process PreparedStatement before executing the query, e.g., set statement parameters
   * @param session
   * @tparam A
   * @return
   */
  @inline
  def list[A](extract: ResultSet => A,
              before: PreparedStatement => Unit = NOP[PreparedStatement])
             (implicit session: DBSession): List[A]
  = query[List[A]](collectList(_, extract), before)(session)


  /**
   *
   * collect results to a vector.
   *
   * @param extract
   * @param before process PreparedStatement before executing the query, e.g., set statement parameters
   * @param session
   * @tparam A
   * @return
   */
  @inline
  def vector[A](extract: ResultSet => A,
                before: PreparedStatement => Unit = NOP[PreparedStatement])
               (implicit session: DBSession): Vector[A]
  = query[Vector[A]](collectVec(_, extract), before)(session)


  /**
   * collect results to a map.
   * each row creates one key & value pair
   *
   * @param extract
   * @param before process PreparedStatement before executing the query, e.g., set statement parameters
   * @param session
   * @tparam K
   * @tparam V
   * @return
   */
  @inline
  def map[K, V](extract: ResultSet => (K, V),
                before: PreparedStatement => Unit = NOP[PreparedStatement])
               (implicit session: DBSession): Map[K, V]
  = query[Map[K, V]](collectMap(_, extract), before)(session)


  /**
   * automatically collect data from the first row.
   * the keys are names of the database columns
   * the values are vectors of values from the first row of the resultSet
   *
   * @param before process PreparedStatement before executing the query, e.g., set statement parameters
   * @param session
   * @return
   */
  def autoMap(before: PreparedStatement => Unit = NOP[PreparedStatement])
             (implicit session: DBSession): Map[String, Any]
  = query[Map[String, Any]](collectToMap(_), before)(session)


  /**
   * collect result to a map of vectors.
   * the keys are strings of names of the database columns,
   * the values are vectors of values from each row of the resultSet
   *
   * @example
   *          {{{
   *            val group = sql"SELECT col_1, col_2 FROM my_table LIMIT 10".autoMap()
   *            // group: Map[String, Vector[Any]]
   *            // map of two entries: 'col_1' -> record_1, record_2, ...
   *            //                     'col_2' -> record_1, record_2, ...
   *          }}}
    * @param before process PreparedStatement before executing the query, e.g., set statement parameters
   * @param session
   * @return
   */
  def autoGroup(before: PreparedStatement => Unit = NOP[PreparedStatement])
             (implicit session: DBSession): Map[String, Vector[Any]]
  = query[Map[String, Vector[Any]]](autoColGroup(_), before)(session)


  def product(before: PreparedStatement => Unit = NOP[PreparedStatement])
           (implicit session: DBSession): Product
  = query[Product](collectToProduct(_), before)(session)

}

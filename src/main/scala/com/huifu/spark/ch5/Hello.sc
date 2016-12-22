try {
  val row = Array("1", "2", "8", "4")
  Some((row(0), row.drop(1).map(_.toDouble)))
} catch {
  case _: Throwable =>
    None
}





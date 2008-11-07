typedef binary ContextData
typedef string Data

struct GetResult {
  1:ContextData context
  2:list<Data> results
}

exception FailureException {
  1:string message
}


service Dynomite {
  GetResult get(1:Data key) throws (1:FailureException fail)

  /**
   * Store a piece of data
   *
   * @return number of servers stored
   */
  i32 put(1:Data key, 2:ContextData context, 3:Data data) throws (1:FailureException fail)


  /**
   * @return number of servers that have this key
   */
  i32 has(1:Data key) throws (1:FailureException fail)

  /**
   * @return the number of servers deleted from
   */
  i32 remove(1:Data key) throws (1:FailureException fail)
}
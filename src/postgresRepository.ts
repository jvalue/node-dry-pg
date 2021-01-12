import { Pool, PoolConfig, QueryResult, ClientBase } from 'pg'

import { sleep, stringifiers } from '@jvalue/node-dry-basics'

export class PostgresRepository {
  private readonly connectionPool: Pool

  constructor (poolConfig?: PoolConfig) {
    this.connectionPool = new Pool(poolConfig)
    // Register an error handler to catch errors when the connection of an idle database client is closed,
    // because of a backend error or a network partition. If those errors are not handled the NodeJS process will exit.
    this.connectionPool.on('error', (err) => console.log('Idle postgres connection errored:', err.message))
  }

  /**
   * Waits till a successfull connection to the database has been established.
   * This methods tries repeatedly to perform the no-op query `SELECT 1` till the query succeeded.
   *
   * Note: There is no guarante that subsequent queries will also succeed, because we
   * are in a distributed system, services and the network can fail at any time!
   *
   * @param retries:  Number of retries to connect to the database
   * @param backoffMs:  Time in ms to backoff before next connection retry
   * @returns reject promise on failure to connect
   */
  public async waitForConnection (retries: number, backoffMs: number): Promise<void> {
    console.debug('Waiting for a database connection')

    let lastError: Error | undefined
    for (let i = 1; i <= retries; i++) {
      try {
        await this.connectionPool.query('SELECT 1')
        console.info('Successfully established connection to database.')
        return
      } catch (error) {
        lastError = error
        console.info(`Failed connecting to database (${i}/${retries})`)
      }
      await sleep(backoffMs)
    }
    throw lastError ?? new Error('Failed to connect to database')
  }

  /**
   * Executes a single query.
   *
   * Multiple calls to this method will use different clients from the internal connection pool.
   * Because PostgreSQL isolates transactions to individual clients, do not use transactions with
   * this method. Instead use the `transaction(...)` method.
   *
   * @param query the query to execute
   * @param args optional parameter for parameterized queries
   */
  public async executeQuery (query: string, args?: unknown[]): Promise<QueryResult> {
    if (args === undefined) {
      args = []
    }

    try {
      const resultSet = await this.connectionPool.query(query, args)
      console.debug(`[Query] "${query}" with values ${stringifiers.stringifyArray(args)} ` +
        `led to ${resultSet.rowCount} results`)
      return resultSet
    } catch (error) {
      console.error(`[Query] "${query}" with values ${stringifiers.stringifyArray(args)} failed:`, error)
      throw error
    }
  }

  /**
   * Executes the given function inside a transaction. The function must return a `Promise`.
   * If the Promise resolves the transaction will be committed, otherwise the transaction will be rollbacked.
   * Only use the database client passed to `func` to perform database operations, otherwise you will
   * loose the transactional guarantees.
   *
   * @param func the `Promise` returning function to execute inside a transaction
   */
  public async transaction (func: (client: ClientBase) => Promise<void>): Promise<void> {
    const client = await this.connectionPool.connect()

    try {
      await client.query('BEGIN')
      await func(client)
      await client.query('COMMIT')
    } catch (error) {
      await client.query('ROLLBACK')
      throw error
    } finally {
      client.release()
    }
  }

  public async close (): Promise<void> {
    await this.connectionPool.end()
  }
}

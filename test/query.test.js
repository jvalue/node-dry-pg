/* eslint-env jest */
const { promisify } = require('util')
const exec = promisify(require('child_process').exec)
const { sleep } = require('@jvalue/node-dry-basics')

const { PostgresRepository } = require('../src/postgresRepository')

const CONTAINER_NAME = 'node-dry-pg-test-database'
const DATABASE_PORT = 5432
const DATABASE_USER = 'node-dry-pg-test'
const DATABASE_PASSWORD = 'test-pwd'
const TEST_TIMEOUT = 30000

const DB_CONNECTION_RETRIES = 10
const DB_CONNECTION_BACKOFF = 2000
const DB_STARTUP_TIME = 5000

const POOL_CONFIG = {
  host: 'localhost',
  port: DATABASE_PORT,
  user: DATABASE_USER,
  password: DATABASE_PASSWORD,
  database: DATABASE_USER
}

async function startDatabase () {
  await exec(`docker run -p ${DATABASE_PORT}:5432 --name ${CONTAINER_NAME} -d ` +
    `-e POSTGRES_USER=${DATABASE_USER} -e POSTGRES_PASSWORD=${DATABASE_PASSWORD} ` +
    'postgres:13-alpine')
}

describe('node-dry-pg query test', () => {
  let postgresRepository

  beforeEach(() => {
    postgresRepository = new PostgresRepository(POOL_CONFIG)
  })

  afterEach(async () => {
    try {
      await postgresRepository.close()
    } catch {}

    try {
      await exec(`docker stop ${CONTAINER_NAME}`)
    } catch {}

    try {
      await exec(`docker rm ${CONTAINER_NAME}`)
    } catch {}
  })

  test('simple query succeeds', async () => {
    await startDatabase()
    await sleep(DB_STARTUP_TIME)

    const result = await postgresRepository.executeQuery('SELECT 1')
    expect(result).toBeDefined()
    expect(result.rowCount).toEqual(1)
  }, TEST_TIMEOUT)

  test('simple query fails', async () => {
    // Explicitly set number of expected assertions so the assertion in the catch block is not missed
    expect.assertions(1)

    await startDatabase()
    await sleep(DB_STARTUP_TIME)

    try {
      await postgresRepository.executeQuery('SELECT * FROM unknown_table')
    } catch (error) {
      expect(error).toHaveProperty('message', 'relation "unknown_table" does not exist')
    }
  }, TEST_TIMEOUT)

  test('waitsForConnection succeeds', async () => {
    const waitForConnectionPromise = postgresRepository.waitForConnection(DB_CONNECTION_RETRIES, DB_CONNECTION_BACKOFF)
    await startDatabase()

    await waitForConnectionPromise

    const result = await postgresRepository.executeQuery('SELECT 1')
    expect(result).toBeDefined()
    expect(result.rowCount).toEqual(1)
  }, TEST_TIMEOUT)

  test('waitsForConnection fails', async () => {
    expect.assertions(1)

    try {
      await postgresRepository.waitForConnection(2, 200)
    } catch (error) {
      expect(error).toHaveProperty('message', 'connect ECONNREFUSED 127.0.0.1:5432')
    }
  }, TEST_TIMEOUT)

  test('handles connection loss', async () => {
    await startDatabase()
    await postgresRepository.waitForConnection(DB_CONNECTION_RETRIES, DB_CONNECTION_BACKOFF)

    await exec(`docker stop ${CONTAINER_NAME}`)
    await exec(`docker start ${CONTAINER_NAME}`)

    await sleep(DB_STARTUP_TIME)

    const result = await postgresRepository.executeQuery('SELECT 1')
    expect(result).toBeDefined()
    expect(result.rowCount).toEqual(1)
  }, TEST_TIMEOUT)

  test('commit transaction', async () => {
    const TABLE_NAME = 'transaction_commit_test'
    await startDatabase()
    await postgresRepository.waitForConnection(DB_CONNECTION_RETRIES, DB_CONNECTION_BACKOFF)

    await postgresRepository.executeQuery(`CREATE TABLE ${TABLE_NAME} (name text)`)
    await postgresRepository.executeQuery(`INSERT INTO ${TABLE_NAME}(name) VALUES ($1)`, ['Test 1'])

    await postgresRepository.transaction(async client => {
      await client.query(`INSERT INTO ${TABLE_NAME}(name) VALUES ($1)`, ['Test 2'])
      await client.query(`INSERT INTO ${TABLE_NAME}(name) VALUES ($1)`, ['Test 3'])

      // From inside the transaction the inserts should be visible
      const result1 = await client.query(`SELECT * FROM ${TABLE_NAME}`)
      expect(result1.rows.map(r => r.name)).toEqual(['Test 1', 'Test 2', 'Test 3'])

      // From outside of the transaction the inserts should not be visible
      const result2 = await postgresRepository.executeQuery(`SELECT * FROM ${TABLE_NAME}`)
      expect(result2.rows.map(r => r.name)).toEqual(['Test 1'])
    })

    const result = await postgresRepository.executeQuery(`SELECT * FROM ${TABLE_NAME}`)
    expect(result.rows.map(r => r.name)).toEqual(['Test 1', 'Test 2', 'Test 3'])
  }, TEST_TIMEOUT)

  test('rollback transaction', async () => {
    // Explicitly set number of expected assertions so the assertion in the catch block is not missed
    expect.assertions(4)

    const TABLE_NAME = 'transaction_rollback_test'
    await startDatabase()
    await postgresRepository.waitForConnection(DB_CONNECTION_RETRIES, DB_CONNECTION_BACKOFF)

    await postgresRepository.executeQuery(`CREATE TABLE ${TABLE_NAME} (name text)`)
    await postgresRepository.executeQuery(`INSERT INTO ${TABLE_NAME}(name) VALUES ($1)`, ['Test 1'])

    try {
      await postgresRepository.transaction(async client => {
        await client.query(`INSERT INTO ${TABLE_NAME}(name) VALUES ($1)`, ['Test 2'])
        await client.query(`INSERT INTO ${TABLE_NAME}(name) VALUES ($1)`, ['Test 3'])

        // From inside the transaction the inserts should be visible
        const result1 = await client.query(`SELECT * FROM ${TABLE_NAME}`)
        expect(result1.rows.map(r => r.name)).toEqual(['Test 1', 'Test 2', 'Test 3'])

        // From outside of the transaction the inserts should not be visible
        const result2 = await postgresRepository.executeQuery(`SELECT * FROM ${TABLE_NAME}`)
        expect(result2.rows.map(r => r.name)).toEqual(['Test 1'])

        throw new Error('Error in database transaction')
      })
    } catch (error) {
      expect(error).toHaveProperty('message', 'Error in database transaction')
    }

    const result = await postgresRepository.executeQuery(`SELECT * FROM ${TABLE_NAME}`)
    expect(result.rows.map(r => r.name)).toEqual(['Test 1'])
  }, TEST_TIMEOUT)
})

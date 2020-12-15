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
    await postgresRepository.close()
    await exec(`docker stop ${CONTAINER_NAME}`)
    await exec(`docker rm ${CONTAINER_NAME}`)
  })

  test('simple query', async () => {
    await startDatabase()
    await sleep(DB_STARTUP_TIME)

    const result = await postgresRepository.executeQuery('SELECT 1', [])
    expect(result).toBeDefined()
    expect(result.rowCount).toEqual(1)
  }, TEST_TIMEOUT)

  test('waitsForConnection', async () => {
    const waitForConnectionPromise = postgresRepository.waitForConnection(DB_CONNECTION_RETRIES, DB_CONNECTION_BACKOFF)
    await startDatabase()

    await waitForConnectionPromise

    const result = await postgresRepository.executeQuery('SELECT 1', [])
    expect(result).toBeDefined()
    expect(result.rowCount).toEqual(1)
  }, TEST_TIMEOUT)

  test('handles connection loss', async () => {
    await startDatabase()
    await postgresRepository.waitForConnection(DB_CONNECTION_RETRIES, DB_CONNECTION_BACKOFF)

    await exec(`docker stop ${CONTAINER_NAME}`)
    await exec(`docker start ${CONTAINER_NAME}`)

    await sleep(DB_STARTUP_TIME)

    const result = await postgresRepository.executeQuery('SELECT 1', [])
    expect(result).toBeDefined()
    expect(result.rowCount).toEqual(1)
  }, TEST_TIMEOUT)
})

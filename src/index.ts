import { PostgresClient } from './postgresClient'

// Export `PostgresClient` also under its old name `PostgresRepository`, so users can update gradually
export { PostgresClient, PostgresClient as PostgresRepository }

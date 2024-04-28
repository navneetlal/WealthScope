import express from 'express'
import helmet from 'helmet'
import cors from 'cors'
import controller from './controllers'

import './jobs/processParsedCasRxjs'
import MongoClientSingleton from './infra/mongo'

const DB_NAME = process.env.DB_NAME ?? 'casparser'

const app = express()

app.use(helmet())

app.use(express.json())
app.use(express.urlencoded({ extended: false }))

app.use(cors())

app.use('/api', controller)

app.get('/', (_req, res) => {
  res.send('Hello World!')
})

app.listen(3000, async () => {
  console.log('Server is running on port 3000')
  const client = await MongoClientSingleton.getInstance()
  const db = client.db(DB_NAME)
  // await Promise.all([
  //   db.createIndex('agenda_jobs', {
  //     "name": 1,
  //     "nextRunAt": 1,
  //     "priority": -1,
  //     "lockedAt": 1,
  //     "disabled": 1
  //   }, { unique: true }),
  //   db.createIndex('transactions', {
  //     "transaction_date": 1,
  //     "transaction_type": 1
  //   }),
  // ])
})

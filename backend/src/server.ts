import express from 'express'
import helmet from 'helmet'
import cors from 'cors'
import controller from './controllers'

import './jobs'

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
})

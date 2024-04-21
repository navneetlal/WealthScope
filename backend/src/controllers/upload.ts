import { Router } from 'express'
import multer from 'multer'
import { customAlphabet } from 'nanoid/non-secure'
import mimeToExtensionMap from '../utility/mimeToExtensionMap'
import { getCollection } from '../dao/mongo'

const nanoid = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTWXYZ', 21)

const router = Router()

const storage = multer.diskStorage({
  destination: function (_req, _file, cb) {
    cb(null, process.env.DIRECTORY_TO_WATCH ?? '/tmp/casparser/')
  },
  filename: function (_req, file, cb) {
    console.log('Received file:', file.originalname)
    cb(null, 'CAS-' + nanoid() + '.' + mimeToExtensionMap[file.mimetype])
  },
})

const upload = multer({ storage: storage })

router.post('/', upload.single('file'), async (req, res) => {
  try {
    const collection = await getCollection('file_credentials')
    const expiresAt = new Date(Date.now() + 30 * 60 * 1000) // Expire in 30 minutes
    const data = {
      file_name: req.file?.filename,
      password: req.body.password,
      created_at: new Date(),
      expires_at: expiresAt,
    }

    await collection.insertOne(data)
    console.log('File credentials stored in MongoDB:', data.file_name)
    res.send('File uploaded and credentials stored successfully!')
  } catch (error) {
    console.error('Error storing file credentials:', error)
    res.status(500).send('Error storing file credentials')
  }
})

export default router

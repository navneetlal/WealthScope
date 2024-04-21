import { Router } from 'express'
import UploadController from './upload'

const router = Router()

// const publicRoutes = [
//     {
//         path: '',
//         controller: ''
//     }
// ]

const privateRoute = [
  {
    path: '/upload',
    controller: UploadController,
  },
]

// publicRoutes.forEach(route => router.use(route.path, route.controller))
privateRoute.forEach((route) => router.use(route.path, route.controller))

export default router

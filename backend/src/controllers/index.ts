import { Router } from 'express'
import UploadController from './upload'
import InvestmentController from './investment'
import TransactionController from './transaction'

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
  {
    path: '/investment',
    controller: InvestmentController,
  },
  {
    path: '/transactions',
    controller: TransactionController,
  }
]

// publicRoutes.forEach(route => router.use(route.path, route.controller))
privateRoute.forEach((route) => router.use(route.path, route.controller))

export default router

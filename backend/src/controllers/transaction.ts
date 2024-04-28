import express from 'express';
import { getCollection } from '../infra/mongo';

const router = express.Router();

// API to list all transactions
router.get('/', async (_req, res) => {
    try {
        const transactionsCollection = await getCollection('transactions');
        const transactions = await transactionsCollection.find({}).toArray();
        res.json(transactions);
    } catch (error) {
        res.status(500).send(error);
    }
});

export default router;

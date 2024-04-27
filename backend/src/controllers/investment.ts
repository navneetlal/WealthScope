import express from 'express';

import calculateAnnualNetAmounts from '../services/calculateAnnualNetAmount';
import calculateTopMovers from '../services/calculateTopMovers';

const router = express.Router();

router.get('/investments/annual-net-amounts', async (_req, res) => {
    try {
        const annualNetAmounts = await calculateAnnualNetAmounts();
        res.json({ annualNetAmounts });
    } catch (error) {
        res.status(500).send(error);
    }
});

router.get('/investments/top-movers', async (_req, res) => {
    try {
        const { topGainers, topLosers } = await calculateTopMovers();
        res.json({ topGainers, topLosers });
    } catch (error) {
        res.status(500).send(error);
    }
});

export default router;

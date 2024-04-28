import express from 'express';

import calculateAnnualNetAmounts from '../services/calculateAnnualNetAmount';
import calculateTopMovers from '../services/calculateTopMovers';

const router = express.Router();

router.get('/annual-net-amounts', async (_req, res) => {
    try {
        const annualNetAmounts = await calculateAnnualNetAmounts();
        const response = annualNetAmounts.map(annualNetAmount => ({
            financial_year: annualNetAmount.year,
            net_amount: annualNetAmount.netAmount.toString(),
        }));
        res.json(response);
    } catch (error) {
        res.status(500).send(error);
    }
});

router.get('/top-movers', async (req, res) => {
    try {
        const { sort_by: sortBy = 'value' } = req.params as Record<string, any>
        const { topGainers, topLosers } = await calculateTopMovers({ sortBy });
        res.json({ topGainers, topLosers });
    } catch (error) {
        res.status(500).send(error);
    }
});

router.get('/total-valuation-timescale', async (req, res) => {
    try {
        const { sort_by: sortBy = 'value' } = req.params as Record<string, any>
        const { topGainers, topLosers } = await calculateTopMovers({ sortBy });
        res.json({ topGainers, topLosers });
    } catch (error) {
        res.status(500).send(error);
    }
});

export default router;

import { getCollection } from '../infra/mongo';

const calculateAnnualNetAmounts = async () => {
    const transactionsCollection = await getCollection('transactions');
  
    const result = await transactionsCollection.aggregate([
      {
        $group: {
          _id: { $year: '$transaction_date' },
          totalInvested: {
            $sum: {
              $cond: [{ $eq: ['$transaction_type', 'PURCHASE_SIP'] }, '$transaction_amount', 0]
            }
          },
          totalWithdrawn: {
            $sum: {
              $cond: [{ $eq: ['$transaction_type', 'WITHDRAWAL'] }, '$transaction_amount', 0]
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          year: '$_id',
          netAmount: { $subtract: ['$totalInvested', '$totalWithdrawn'] }
        }
      },
      { $sort: { year: 1 } }
    ]).toArray();
  
    return result;
  };

  export default calculateAnnualNetAmounts
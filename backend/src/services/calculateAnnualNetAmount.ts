import { getCollection } from '../infra/mongo';

const calculateAnnualNetAmounts = async () => {
    const transactionsCollection = await getCollection('transactions');
  
    const result = await transactionsCollection.aggregate([
        {
            $addFields: {
              transaction_date: {
                $dateFromString: {
                  dateString: '$transaction_date',
                  format: '%Y-%m-%d'
                }
              },
              transaction_amount: { $toDecimal: "$transaction_amount" },
            }
          },
          {
            $project: {
            financial_year: {
                $cond: {
                  if: { $gte: [{ $month: '$transaction_date' }, 4] },
                  then: { $year: '$transaction_date' },
                  else: { $subtract: [{ $year: '$transaction_date' }, 1] }
                }
              },
              transaction_type: 1,
              transaction_amount: 1
            }
          },
          {
            $group: {
              _id: '$financial_year',
              totalInvested: {
                $sum: {
                  $cond: [
                    {
                      $in: ['$transaction_type', ['PURCHASE', 'PURCHASE_SIP', 'DIVIDEND_REINVESTMENT', 'SWITCH_IN']]
                    },
                    '$transaction_amount',
                    0
                  ]
                }
              },
              totalWithdrawn: {
                $sum: {
                  $cond: [
                    {
                      $in: ['$transaction_type', ['REDEMPTION', 'SWITCH_OUT', 'DIVIDEND_PAYOUT', 'SEGREGATION']]
                    },
                    '$transaction_amount',
                    0
                  ]
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
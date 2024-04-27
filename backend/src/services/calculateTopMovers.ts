import { getCollection } from '../infra/mongo';

const calculateTopMovers = async () => {
    const valuationCollection = await getCollection('valuation_per_amfi');
    const schemesCollection = await getCollection('schemes');
  


    // Find the last two valuations for each amfi code
    const valuations = await valuationCollection.aggregate([
      {
        $sort: { date: -1 }
      },
      {
        $group: {
          _id: '$amfi',
          valuations: { $push: '$$ROOT' }
        }
      },
      {
        $project: {
          _id: 0,
          amfi: '$_id',
          lastValuation: { $arrayElemAt: ['$valuations', 0] },
          secondLastValuation: { $arrayElemAt: ['$valuations', 1] }
        }
      },
      {
        $project: {
          amfi: 1,
          change: {
            $subtract: [
              '$lastValuation.totalValuation',
              '$secondLastValuation.totalValuation'
            ]
          },
          percentageChange: {
            $cond: [
              { $eq: ['$secondLastValuation.totalValuation', 0] },
              0,
              {
                $multiply: [
                  {
                    $divide: [
                      {
                        $subtract: [
                          '$lastValuation.totalValuation',
                          '$secondLastValuation.totalValuation'
                        ]
                      },
                      '$secondLastValuation.totalValuation'
                    ]
                  },
                  100
                ]
              }
            ]
          }
        }
      }
    ]).toArray();
  
    // Sort the results to find the top gainers and losers
    const topGainers = [...valuations].sort((a, b) => b.percentageChange - a.percentageChange).slice(0, 5);
    const topLosers = [...valuations].sort((a, b) => a.percentageChange - b.percentageChange).slice(0, 5);
  
    // Map the amfi codes to the scheme names
    const schemeNames = await schemesCollection.find({}).project({ amfi: 1, scheme: 1 }).toArray();
    const amfiToSchemeMap = schemeNames.reduce((acc, scheme) => {
      acc[scheme.amfi] = scheme.scheme;
      return acc;
    }, {});
  
    // Add scheme names to the top gainers and losers
    topGainers.forEach(gainer => gainer.schemeName = amfiToSchemeMap[gainer.amfi]);
    topLosers.forEach(loser => loser.schemeName = amfiToSchemeMap[loser.amfi]);
  
    return { topGainers, topLosers };
  };

  export default calculateTopMovers
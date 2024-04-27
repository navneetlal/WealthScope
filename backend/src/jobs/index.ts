import Agenda, { Job } from 'agenda'
import { getCollection } from '../infra/mongo'
import axios from 'axios'
import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat'

dayjs.extend(customParseFormat)

const mongoConnectionString = (process.env.MONGO_URI ?? 'mongodb://localhost:27017') + '/' + (process.env.DB_NAME ?? 'casparser')
const agenda = new Agenda({ db: { address: mongoConnectionString, collection: 'agenda_jobs' } })

  ; (async function () {
    agenda.define('process parsed_cas_data', async (job: Job) => {
      const { file_name } = job.attrs.data
      const parsedCasDataCollection = await getCollection('parsed_cas_data')
      const parsedCasData = await parsedCasDataCollection.findOne(
        { file_name },
        { projection: { 'data.folios.schemes.transactions': 0 } }
      )
      const schemes = parsedCasData?.data?.folios.flatMap(({ schemes, ...rest }: any) =>
        schemes.map((scheme: any) => ({ ...rest, ...scheme }))
      )
      try {
        const schemeCollection = await getCollection('schemes')
        const result = await schemeCollection.bulkWrite(
          schemes.map((scheme: any) => ({
            updateOne: {
              filter: { amfi: scheme.amfi },
              update: { $set: scheme },
              upsert: true,
            },
          }))
        )
        console.log(`Upserted ${result.upsertedCount} documents`)
      } catch (error) {
        console.error('Error upserting documents:', error)
      }

      try {
        const transactionCollection = await getCollection('transactions')
        const transactions = await parsedCasDataCollection
          .aggregate([
            {
              $unwind: '$data.folios',
            },
            {
              $unwind: '$data.folios.schemes',
            },
            {
              $unwind: '$data.folios.schemes.transactions',
            },
            {
              $project: {
                scheme_name: '$data.folios.schemes.scheme',
                advisor: '$data.folios.schemes.advisor',
                rta_code: '$data.folios.schemes.rta_code',
                rta: '$data.folios.schemes.rta',
                type: '$data.folios.schemes.type',
                isin: '$data.folios.schemes.isin',
                amfi: '$data.folios.schemes.amfi',
                transaction_date: '$data.folios.schemes.transactions.date',
                transaction_description: '$data.folios.schemes.transactions.description',
                transaction_amount: '$data.folios.schemes.transactions.amount',
                transaction_units: '$data.folios.schemes.transactions.units',
                transaction_nav: '$data.folios.schemes.transactions.nav',
                transaction_balance: '$data.folios.schemes.transactions.balance',
                transaction_type: '$data.folios.schemes.transactions.type',
                transaction_dividend_rate: '$data.folios.schemes.transactions.dividend_rate',
                _id: 0,
              },
            },
          ])
          .toArray()
        const result = await transactionCollection.bulkWrite(
          transactions.map((transaction: any) => ({
            updateOne: {
              filter: {
                amfi: transaction.amfi,
                transaction_date: transaction.transaction_date,
                transaction_type: transaction.transaction_type,
                transaction_unit: transaction.transaction_units,
                transaction_amount: transaction.transaction_amount,
                transaction_balance: transaction.transaction_balance,
              },
              update: { $set: transaction },
              upsert: true,
            },
          }))
        )
        console.log(`Upserted ${result.insertedCount} documents`)
      } catch (error) {
        console.error('Error upserting documents:', error)
      }

      schemes.map((scheme: any) =>
        agenda.now('populate amfi navs', {
          ...job.attrs.data,
          amfi: scheme.amfi,
          firstTransactionDate: scheme.firstTransactionDate,
        })
      )
    })

    agenda.define('populate amfi navs', async (job: Job) => {
      const { amfi } = job.attrs.data
      const data = await axios.get(`https://api.mfapi.in/mf/${amfi}`)

      const navs = data.data.data.map((nav: any) => {
        return ({ ...data.data.meta, ...nav, date: dayjs(nav.date, "DD-MM-YYYY").format('YYYY-MM-DD') })
      })
      console.log(navs.length)
      try {
        const navCollection = await getCollection('navs')
        const result = await navCollection.bulkWrite(
          navs.map((nav: any) => ({
            updateOne: {
              filter: { amfi: nav.scheme_code.toString(), date: nav.date },
              update: { $set: nav },
              upsert: true,
            },
          }))
        )
        console.log(`Upserted ${result.upsertedCount} documents`)
      } catch (error) {
        console.error('Error upserting documents:', error)
      }
      try {
        const schemeCollection = await getCollection('schemes')
        const result = await schemeCollection.updateOne(
          { amfi: navs[0].scheme_code.toString() },
          { $set: { scheme_category: navs[0].scheme_category, scheme_type: navs[0].scheme_type } }
        )
        console.log(`Updated ${result.modifiedCount} schemes`)
      } catch (error) {
        console.error('Error upserting documents:', error)
      }
      await agenda.now('create per day valuation per amfi', { amfi })
    })

    agenda.define('create per day valuation per amfi', async (job: Job) => {
      const transactionCollection = await getCollection('transactions')
      const navCollection = await getCollection('navs')
      const transactions = await transactionCollection.find({ amfi: job.attrs.data.amfi }).sort({ transaction_date: 1 }).toArray()
      console.log(transactions[0])
      const navs = await navCollection.find({ amfi: job.attrs.data.amfi, date: { $gte: transactions[0]?.transaction_date } }).sort({ date: 1 }).toArray()
      const transactionMap = transactions.reduce((acc, transaction) => {
        const date = transaction.transaction_date;
        if (date) {
          if (!acc.has(date)) {
            acc.set(date, []);
          }
          acc.get(date).push(transaction);
        } else {
          console.warn('Transaction with missing or undefined date:', transaction);
        }
        return acc;
      }, new Map());
      let totalAmount = 0;
      let totalUnits = 0;

      let valuationArray = navs.map(nav => {
        const date = nav.date;
        const transactionsForDate = transactionMap.get(date) || [];

        transactionsForDate.forEach((transaction: any) => {
          const amount = parseFloat(transaction.transaction_amount) || 0;
          const units = parseFloat(transaction.transaction_units) || 0;
          totalAmount += amount;
          totalUnits += units;
        });

        const navValue = parseFloat(nav.nav);
        const totalValuation = totalUnits * navValue;
        return {
          updateOne: {
            filter: { amfi: nav.amfi, date },
            update: { $set: {
              amfi: nav.amfi,
              date: date,
              totalAmount: totalAmount,
              totalUnits: totalUnits,
              nav: navValue,
              totalValuation: totalValuation
            } },
            upsert: true,
          },
        }
      });

      try {
        const schemeCollection = await getCollection('valuations_per_amfi')
        const result = await schemeCollection.bulkWrite(valuationArray)
        console.log(`Upserted ${result.upsertedCount} documents`)
      } catch (error) {
        console.error('Error upserting documents:', error)
      }

      // console.log(valuationArray);

    })

    await agenda.start()

    await agenda.now('process parsed_cas_data', {
      file_name: '71216576420230747V01118364674972CPIMBCP162658711.pdf',
    })

    // await agenda.now('fetch amfi navs', { amfi: '120251' })

    // await agenda.now('create per day valuation', { amfi: '120251' })

    await agenda.every('*/1 * * * *', 'delete old users')
  })()

agenda.on('start', (job) => {
  console.log(`Job ${job.attrs.name} starting with params: ${JSON.stringify(job.attrs.data)}`)
})

agenda.on('complete', (job) => {
  console.log(`Job ${job.attrs.name} finished for params: ${JSON.stringify(job.attrs.data)}`)
})

export default agenda

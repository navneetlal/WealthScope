import axios from 'axios';
import Agenda, { type Job } from 'agenda';
import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';
import { Observable, forkJoin, from, of } from 'rxjs';
import { mergeMap, filter, concatMap, tap, map, catchError, reduce } from 'rxjs/operators';
import { type UpdateResult, type BulkWriteResult, type Collection, type Document } from 'mongodb';

import { getCollection } from '../infra/mongo';

dayjs.extend(customParseFormat);

const mongoConnectionString = (process.env.MONGO_URI ?? 'mongodb://localhost:27017') + '/' + (process.env.DB_NAME ?? 'casparser');
const agenda = new Agenda({ db: { address: mongoConnectionString, collection: 'agenda_jobs' } });

function isNotNull<T>(value: T | null): value is T {
  return value !== null;
}

agenda.define('process parsed_cas_data', async (_job: Job) => {

  const parsedCasDataCollection$ = from(getCollection('parsed_cas_data').then(collection =>
    collection.find({ locked: { $ne: 'true' }, status: { $ne: 'hh' } }, { projection: { file_name: 1 } }).toArray()
  ));

  parsedCasDataCollection$.pipe(
    concatMap(parsedCasDataList => from(parsedCasDataList)),
    tap(parsedCasData => console.log(`Starting processing of file ${parsedCasData.file_name}.`)),
    mergeMap(file => {
      return from(getCollection('parsed_cas_data').then(collection =>
        collection.findOneAndUpdate(
          { file_name: file.file_name },
          { $set: { locked: true, status: 'processing' } },
          { projection: { 'data.folios.schemes.transactions': 0 } }
        )
      )).pipe(
        filter(isNotNull),
        tap(parsedCasData => console.log(`Locked ${parsedCasData.file_name} for processing.`)),
        map((parsedCasData): any[] => parsedCasData?.data?.folios.flatMap(({ schemes, ...rest }: any) =>
          schemes.map((scheme: any) => ({ ...rest, ...scheme }))
        )),
        mergeMap((schemes): Observable<{ schemes: any[], collection: Collection }> => forkJoin({
          schemes: of(schemes),
          collection: getCollection('schemes')
        }) as Observable<{ schemes: any[], collection: Collection }>),
        mergeMap(({ schemes, collection }): Observable<{ schemes: any[], result: BulkWriteResult }> => {
          return forkJoin({
            schemes: of(schemes),
            result: collection.bulkWrite(
              schemes.map((scheme: any) => ({
                updateOne: {
                  filter: { amfi: scheme.amfi },
                  update: { $set: scheme },
                  upsert: true,
                },
              }))
            )
          }) as Observable<{ schemes: any[], result: BulkWriteResult }>
        }),
        tap(({ result }) => console.log(`Added ${result.upsertedCount} new schemes & Updated ${result.modifiedCount} schemes.`)),
        mergeMap(({ schemes }): Observable<{ schemes: any[], collection: Collection }> => forkJoin({ schemes: of(schemes), collection: getCollection('parsed_cas_data') })),
        mergeMap(({ schemes, collection }): Observable<{ schemes: any[], result: Document[] }> => forkJoin({
          schemes: of(schemes),
          result: collection.aggregate([
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
          ]).toArray()
        }) as Observable<{ schemes: any[], result: Document[] }>),
        mergeMap(({ schemes, result }): Observable<{ schemes: any[], transactions: Document[], collection: Collection }> => forkJoin({
          schemes: of(schemes),
          transactions: of(result),
          collection: getCollection('transactions')
        }) as Observable<{ schemes: any[], transactions: Document[], collection: Collection }>),
        mergeMap(({ schemes, transactions, collection }): Observable<{ schemes: any[], result: BulkWriteResult }> => {
          return forkJoin({
            schemes: of(schemes),
            result: collection.bulkWrite(
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
          }) as Observable<{ schemes: any[], result: BulkWriteResult }>
        }),
        tap(({ result }) => console.log(`Added ${result.upsertedCount} new transactions & Updated ${result.modifiedCount} transactions.`)),
        mergeMap(({ schemes }) => {
          return from(schemes)
            .pipe(
              mergeMap((scheme: any) => axios.get(`https://api.mfapi.in/mf/${scheme.amfi}`)),
              map(({ data }): any[] => data.data.map((nav: any) => {
                return ({ ...data.meta, ...nav, date: dayjs(nav.date, "DD-MM-YYYY").format('YYYY-MM-DD') })
              })),
              mergeMap((navs): Observable<{ navs: any[], collection: Collection }> => forkJoin({
                navs: of(navs),
                collection: getCollection('navs')
              })),
              mergeMap(({ navs, collection }): Observable<{ navs: any[], result: BulkWriteResult }> => {
                return forkJoin({
                  navs: of(navs),
                  result: collection.bulkWrite(
                    navs.map((nav: any) => ({
                      updateOne: {
                        filter: { amfi: nav.scheme_code.toString(), date: nav.date },
                        update: { $set: nav },
                        upsert: true,
                      },
                    }))
                  )
                }) as Observable<{ navs: any[], result: BulkWriteResult }>
              }),
              tap(({ result }) => console.log(`Added ${result.upsertedCount} new navs & Updated ${result.modifiedCount} navs.`)),
              mergeMap(({ navs }): Observable<{ navs: any[], collection: Collection }> => forkJoin({
                navs: of(navs),
                collection: getCollection('schemes')
              }) as Observable<{ navs: any[], collection: Collection }>),
              mergeMap(({ navs, collection }): Observable<{ navs: any[], result: UpdateResult }> => forkJoin({
                navs: of(navs),
                result: collection.updateOne(
                  { amfi: navs[0].scheme_code.toString() },
                  { $set: { scheme_category: navs[0].scheme_category, scheme_type: navs[0].scheme_type } }
                )
              }) as Observable<{ navs: any[], result: UpdateResult }>),
              tap(({ navs }) => console.log(`Updated scheme category & type for amfi ${navs[0].scheme_code.toString()}.`)),
              mergeMap(({ navs }): Observable<{ amfi: string, transactionCollection: Collection }> => forkJoin({
                amfi: of(navs[0].scheme_code.toString()),
                transactionCollection: getCollection('transactions')
              })),
              mergeMap(({ amfi, transactionCollection }): Observable<{ amfi: string, transactions: Document[] }> => forkJoin({
                amfi: of(amfi),
                transactions: transactionCollection.find({ amfi }).sort({ transaction_date: 1 }).toArray()
              }) as Observable<{ amfi: string, transactions: Document[] }>),
              mergeMap(({ amfi, transactions }: { amfi: string, transactions: Document[] }): Observable<{ amfi: string, transactions: Document, collection: Collection }> => forkJoin({
                amfi: of(amfi),
                transactions: of(transactions),
                collection: getCollection('navs')
              })),
              mergeMap(({ amfi, transactions, collection }: { amfi: string, transactions: Document[], collection: Collection }): Observable<{ amfi: string, transactions: Document, navs: Document }> => forkJoin({
                amfi: of(amfi),
                transactions: of(transactions),
                navs: collection.find({ amfi, date: { $gte: transactions[0]?.transaction_date } }).sort({ date: 1 }).toArray()
              })),
              mergeMap(({ amfi, transactions, navs }) => {
                const transactionMap = transactions.reduce((acc: any, transaction: any) => {
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

                let holdings: any[] = []

                transactions.filter((transaction: any) => transaction.transaction_date < navs[0].date).forEach((transaction: any) => {
                  if (transaction.transaction_type === 'REDEMPTION') {
                    let unitsToSell = 0 - parseFloat(transaction.transaction_unit ?? '0');
                    while (unitsToSell > 0 && holdings.length > 0) {
                      let holding = holdings[0];
                      if (unitsToSell >= holding.units) {
                        totalAmount -= holding.cost
                        unitsToSell -= holding.units;
                        holdings.shift();
                      } else {
                        totalAmount -= (holding.nav * unitsToSell);
                        holdings[0].units -= unitsToSell;
                        unitsToSell = 0;
                      }
                    }
                    totalUnits += parseFloat((transaction.transaction_unit ?? '0'))
                  } else {
                    const amount = parseFloat(transaction.transaction_amount) || 0;
                    totalAmount += amount;
                    totalUnits += parseFloat((transaction.transaction_unit ?? '0'))
                    holdings.push({
                      units: parseFloat(transaction.transaction_unit ?? '0'),
                      cost: parseFloat(transaction.transaction_amount ?? '0'),
                      nav: parseFloat(transaction.transaction_nav ?? '0'),
                    });
                  }
                });

                let valuationArray = navs.map((nav: any) => {
                  const date = nav.date;
                  const transactionsForDate = transactionMap.get(date) || [];

                  transactionsForDate.forEach((transaction: any) => {
                    if (transaction.transaction_type === 'REDEMPTION') {
                      let unitsToSell = 0 - parseFloat(transaction.transaction_unit ?? '0');
                      while (unitsToSell > 0 && holdings.length > 0) {
                        let holding = holdings[0];
                        if (unitsToSell >= holding.units) {
                          totalAmount -= holding.cost
                          unitsToSell -= holding.units;
                          holdings.shift();
                        } else {
                          totalAmount -= (holding.nav * unitsToSell);
                          holdings[0].units -= unitsToSell;
                          unitsToSell = 0;
                        }
                      }
                      totalUnits += parseFloat((transaction.transaction_unit ?? '0'))
                    } else {
                      const amount = parseFloat(transaction.transaction_amount) || 0;
                      totalAmount += amount;
                      totalUnits += parseFloat((transaction.transaction_unit ?? '0'))
                      holdings.push({
                        units: parseFloat(transaction.transaction_unit ?? '0'),
                        cost: parseFloat(transaction.transaction_amount ?? '0'),
                        nav: parseFloat(transaction.transaction_nav ?? '0'),
                      });
                    }
                  });

                  const navValue = parseFloat(nav.nav);
                  const totalValuation = totalUnits * navValue;
                  return {
                    updateOne: {
                      filter: { amfi: nav.amfi, date },
                      update: {
                        $set: {
                          amfi: nav.amfi,
                          date: date,
                          totalAmount: totalAmount,
                          totalUnits: totalUnits,
                          nav: navValue,
                          totalValuation: totalValuation
                        }
                      },
                      upsert: true,
                    },
                  }
                });
                return of({
                  amfi,
                  valuationArray
                })
              }),
              mergeMap(({ amfi, valuationArray }) => forkJoin({
                amfi: of(amfi),
                valuationArray: of(valuationArray),
                collection: getCollection('valuations')
              })),
              mergeMap(({ amfi, valuationArray, collection }) => forkJoin({ amfi, result: collection.bulkWrite(valuationArray) })),
              tap(({ result }) => console.log(`Added ${result.upsertedCount} new valuations & Updated ${result.modifiedCount} valuations.`)),
              reduce(() => of(null))
            )
        }),
        tap(() => console.log(`Completed processing file ${file.file_name}.`)),
        mergeMap(() => getCollection('parsed_cas_data')),
        mergeMap(collection => collection.updateOne({ _id: file._id }, { $set: { locked: false, status: 'completed' } })),
        tap(() => console.log(`Unlocked file ${file.file_name}`)),
        catchError(async err => {
          console.log(err)
          const collection = await getCollection('parsed_cas_data')
          collection.updateOne({ _id: file._id }, { $set: { locked: false, status: 'failed' } })
          console.log(`Failed processing file ${file.file_name}.`)
          console.log(`Unlocked file ${file.file_name}`)
          of(err)
        })
      )
    })
  ).subscribe({
    error: err => console.error(err),
    complete: () => console.log('completed')
  })
});

(async function () {
  await agenda.start();
  // await agenda.every('5 minutes', 'process parsed_cas_data', {});

  await agenda.now('process parsed_cas_data', {});

  agenda.on('start', (job) => {
    console.log(`Job ${job.attrs.name} starting`)
  })

  agenda.on('complete', (job) => {
    console.log(`Job ${job.attrs.name} finished`)
  })
})()

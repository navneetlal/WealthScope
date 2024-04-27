import { Observable, forkJoin, from, of } from 'rxjs';
import { mergeMap, filter, delay, concatMap } from 'rxjs/operators';
import { getCollection } from '../infra/mongo';
import Agenda, { type Job } from 'agenda';
import { type UpdateResult, type BulkWriteResult, type Collection, type Document } from 'mongodb';
import axios from 'axios';
import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';

dayjs.extend(customParseFormat);

const mongoConnectionString = (process.env.MONGO_URI ?? 'mongodb://localhost:27017') + '/' + (process.env.DB_NAME ?? 'casparser');
const agenda = new Agenda({ db: { address: mongoConnectionString, collection: 'agenda_jobs' } });

function isNotNull<T>(value: T | null): value is T {
    return value !== null;
}

agenda.define('process parsed_cas_data', async (_job: Job) => {

    const parsedCasDataCollection$ = from(getCollection('parsed_cas_data').then(collection =>
        collection.find({ locked: { $ne: true }, status: { $ne: 'completed' } }, { projection: { file_name: 1 } }).toArray()
    ));

    parsedCasDataCollection$.pipe(
        concatMap(parsedCasDataList => from(parsedCasDataList)),
        delay(1000),
        mergeMap(file => {
            return from(getCollection('parsed_cas_data').then(collection =>
                collection.findOneAndUpdate(
                    { file_name: file.file_name },
                    { $set: { locked: true, status: 'processing' } },
                    { projection: { 'data.folios.schemes.transactions': 0 } }
                )
            )).pipe(
                filter(isNotNull),
                mergeMap((parsedCasData): Observable<any[]> => parsedCasData?.data?.folios.flatMap(({ schemes, ...rest }: any) =>
                    schemes.map((scheme: any) => ({ ...rest, ...scheme }))
                )),
                mergeMap((schemes): Observable<{ schemes: any[], collection: Collection }> => forkJoin({
                    schemes,
                    collection: getCollection('schemes')
                })),
                mergeMap(({ schemes, collection }): Observable<{ schemes: any[], result: BulkWriteResult }> => {
                    return forkJoin({
                        schemes,
                        result: collection.bulkWrite(
                            schemes.map((scheme: any) => ({
                                updateOne: {
                                    filter: { amfi: scheme.amfi },
                                    update: { $set: scheme },
                                    upsert: true,
                                },
                            }))
                        )
                    })
                }),
                mergeMap(({ schemes }): Observable<{ schemes: any[], collection: Collection }> => forkJoin({ schemes, collection: getCollection('transactions') })),
                mergeMap(({ schemes, collection }): Observable<{ schemes: any[], result: Document[] }> => forkJoin({
                    schemes,
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
                })
                ),
                mergeMap(({ schemes, result }): Observable<{ schemes: any[], transactions: Document, collection: Collection }> => forkJoin({
                    schemes,
                    transactions: result,
                    collection: getCollection('parsed_cas_data')
                })),
                mergeMap(({ schemes, transactions, collection }): Observable<{ schemes: any[], result: BulkWriteResult }> => {
                    return forkJoin({
                        schemes,
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
                    })
                }),
                mergeMap(({ schemes }) => {
                    return from(schemes)
                        .pipe(
                            delay(1000),
                            mergeMap(scheme => axios.get(`https://api.mfapi.in/mf/${scheme.amfi}`)),
                            mergeMap(({ data }): Observable<any[]> => data.data.map((nav: any) => {
                                return ({ ...data.data.meta, ...nav, date: dayjs(nav.date, "DD-MM-YYYY").format('YYYY-MM-DD') })
                            })),
                            mergeMap((navs): Observable<{ navs: any[], collection: Collection }> => forkJoin({
                                navs,
                                collection: getCollection('navs')
                            })),
                            mergeMap(({ navs, collection }): Observable<{ navs: any[], result: BulkWriteResult }> => forkJoin({
                                navs,
                                result: collection.bulkWrite(
                                    navs.map((nav: any) => ({
                                        updateOne: {
                                            filter: { amfi: nav.scheme_code.toString(), date: nav.date },
                                            update: { $set: nav },
                                            upsert: true,
                                        },
                                    }))
                                )
                            })),
                            mergeMap(({ navs }): Observable<{ navs: any[], collection: Collection }> => forkJoin({
                                navs,
                                collection: getCollection('schemes')
                            })),
                            mergeMap(({ navs, collection }): Observable<{ navs: any[], result: UpdateResult }> => forkJoin({
                                navs, result: collection.updateOne(
                                    { amfi: navs[0].scheme_code.toString() },
                                    { $set: { scheme_category: navs[0].scheme_category, scheme_type: navs[0].scheme_type } }
                                )
                            })),
                            mergeMap(({ navs }): Observable<{ amfi: string, transactionCollection: Collection }> => forkJoin({
                                amfi: navs[0].scheme_code.toString() as string,
                                transactionCollection: getCollection('transactions')
                            })),
                            mergeMap(({ amfi, transactionCollection }): Observable<{ amfi: string, transactions: Document[] }> => forkJoin({
                                amfi,
                                transactions: transactionCollection.find({ amfi }).sort({ transaction_date: 1 }).toArray()
                            })),
                            mergeMap(({ amfi, transactions }: { amfi: string, transactions: Document[] }): Observable<{ amfi: string, transactions: Document, collection: Collection }> => forkJoin({
                                amfi,
                                transactions,
                                collection: getCollection('schemes')
                            })),
                            mergeMap(({ amfi, transactions, collection }: { amfi: string, transactions: Document[], collection: Collection }): Observable<{ amfi: string, transactions: Document, navs: Document }> => forkJoin({
                                amfi,
                                transactions,
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

                                let valuationArray = navs.map((nav: any) => {
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
                                amfi,
                                valuationArray,
                                collection: getCollection('valuations')
                            })),
                            mergeMap(({ amfi, valuationArray, collection }) => forkJoin({ amfi, result: collection.bulkWrite(valuationArray) })),
                        )
                }),
                mergeMap(() => getCollection('parsed_cas_data')),
                mergeMap(collection => collection.updateOne({ _id: file._id }, { $set: { locked: false, status: 'completed' } })),
            )

        })
    ).subscribe()

});
``
(async function () {
    await agenda.start();
    await agenda.now('process parsed_cas_data', {});
})()

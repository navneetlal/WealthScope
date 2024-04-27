import { type Collection, MongoClient } from 'mongodb'

const MONGO_URI = process.env.MONGO_URI ?? 'mongodb://localhost:27017'
const DB_NAME = process.env.DB_NAME ?? 'casparser'

class MongoClientSingleton {
  private static client: MongoClient | null = null

  private constructor() {}

  public static async getInstance(): Promise<MongoClient> {
    if (!MongoClientSingleton.client) {
      try {
        MongoClientSingleton.client = await MongoClient.connect(MONGO_URI, {
          appName: 'cas-parser',
          maxPoolSize: 50,
          minPoolSize: 5,
          readPreference: 'secondaryPreferred',
        })
      } catch (error) {
        console.error('Error connecting to MongoDB:', error)
        throw error
      }
    }

    return MongoClientSingleton.client
  }

  public static async disconnect(): Promise<void> {
    if (MongoClientSingleton.client) {
      await MongoClientSingleton.client.close()
      MongoClientSingleton.client = null
    }
  }
}

export async function getCollection(collectionName: string): Promise<Collection> {
  const client = await MongoClientSingleton.getInstance()
  const db = client.db(DB_NAME)
  return db.collection(collectionName)
}

export default MongoClientSingleton

import fetch from 'node-fetch';
import { Client, Databases } from "appwrite";

// const { Client, Database } = appwrite;

// Initialize Appwrite client
let client = new Client();
let database = new Database(client);

client
    .setProject(process.env.PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

export default async function fetchAndSaveRates() {
    try {
        const response = await fetch(`https://api.currencyapi.com/v3/latest?apikey=${process.env.RATES_API_KEY}`);
        const rates = json.data;

        const ratesArray = Object.keys(rates).map(key => [key, rates[key].value]);

        // Format the date as DDMMYYYY
        const dateStr = new Date().toLocaleDateString('en-GB').replace(/\//g, '');

        // Check if a document with this date already exists
        let searchResponse = await database.listDocuments(process.env.COLLECTION_ID, [`date=${dateStr}`]);
        let documentId = searchResponse.documents.length > 0 ? searchResponse.documents[0].$id : null;

        let document = {
            date: dateStr,
            jsonRates: JSON.stringify(rates)
        };

        if (documentId) {
            // Update the existing document
            await database.updateDocument(process.env.COLLECTION_ID, documentId, document);
        } else {
            // Create a new document
            await database.createDocument(process.env.COLLECTION_ID, document);
        }
    } catch (error) {
        console.error('Error fetching or saving rates:', error);
    }
}

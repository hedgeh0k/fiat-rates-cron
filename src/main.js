import axios from 'axios';
import { Client, Databases, Query } from "node-appwrite";

export default async function fetchAndSaveRates(context) {
    try {
        const client = new Client();
        const database = new Databases(client);
        client.setEndpoint("https://cloud.appwrite.io/v1");
        client.setProject(process.env.PROJECT_ID);
        client.setKey(process.env.APPWRITE_API_KEY);

        const response = await axios.get(`https://api.currencyapi.com/v3/latest?apikey=${process.env.RATES_API_KEY}`);
        const rates = response?.data?.data ?? {};

        console.log("Retrieved rates", rates);

        const ratesArray = Object.keys(rates).map(key => [key, rates[key].value]);
        const DDMMYYYY = new Date().toLocaleDateString("en-GB").replace(/\//g, "");

        // Check if a document with this date already exists
        let searchResponse = await database.listDocuments(
            process.env.DATABASE_ID,
            process.env.COLLECTION_ID,
            [
                Query.equal("date", DDMMYYYY)
            ]
        );

        let documentId = searchResponse.documents.length > 0 ? searchResponse.documents[0].$id : null;

        const document = {
            date: DDMMYYYY,
            jsonRates: JSON.stringify(ratesArray)
        };
        if (documentId) {
            console.log("Updating existing:", documentId, document);
            // Update the existing document
            await database.updateDocument(process.env.DATABASE_ID, process.env.COLLECTION_ID, documentId, document);
        } else {
            console.log("Saving new:", documentId, document);
            // Create a new document
            await database.createDocument(process.env.DATABASE_ID, process.env.COLLECTION_ID, documentId, document);
        }
        console.log("Done:", documentId);
        return context?.res ? context.res.json({ ok: true, rates: rates }) : undefined;
    } catch (error) {
        console.error("Error fetching or saving rates:", error);
        return context?.res ? context.res.json({ ok: false, error: error }) : undefined;
    }
}

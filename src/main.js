const fetch = require('node-fetch');
const sdk = require('node-appwrite');

// Initialize Appwrite client
let client = new sdk.Client();
let database = new sdk.Database(client);

client
    .setEndpoint('http://[YOUR_APPWRITE_ENDPOINT]') // Your Appwrite Endpoint
    .setProject('YOUR_PROJECT_ID') // Your project ID
    .setKey('YOUR_API_KEY'); // Your secret API key

export default async function fetchAndSaveRates() {
    try {
        const response = await fetch('[FIAT_RATES_API_ENDPOINT]');
        const rates = await response.json();

        // Format the date as DDMMYYYY
        const dateStr = new Date().toLocaleDateString('en-GB').replace(/\//g, '');

        // Check if a document with this date already exists
        let searchResponse = await database.listDocuments('YOUR_COLLECTION_ID', [`date=${dateStr}`]);
        let documentId = searchResponse.documents.length > 0 ? searchResponse.documents[0].$id : null;

        let document = {
            date: dateStr,
            rates: JSON.stringify(rates)
        };

        if (documentId) {
            // Update the existing document
            await database.updateDocument('YOUR_COLLECTION_ID', documentId, document);
        } else {
            // Create a new document
            await database.createDocument('YOUR_COLLECTION_ID', document);
        }
    } catch (error) {
        console.error('Error fetching or saving rates:', error);
    }
}

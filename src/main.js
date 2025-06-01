import { Client, Databases, Query, ID } from "node-appwrite";

export default async function fetchAndSaveRates(context) {
    try {
        const client = new Client();
        const database = new Databases(client);
        client.setEndpoint("https://cloud.appwrite.io/v1");
        client.setProject(process.env.PROJECT_ID);
        client.setKey(process.env.APPWRITE_API_KEY);

        const currencyResponse = await fetch(
            `https://api.currencyapi.com/v3/latest?apikey=${process.env.RATES_API_KEY}`
        );
        const currencyJson = await currencyResponse.json();
        const rates = currencyJson?.data ?? {};

        console.log("Retrieved rates", rates);

        const ratesArray = Object.keys(rates).map((key) => [
            key,
            rates[key].value,
        ]);
        const DDMMYYYY = new Date()
            .toLocaleDateString("en-GB")
            .replace(/\//g, "");

        // Check if a document with this date already exists
        let searchResponse = await database.listDocuments(
            process.env.DATABASE_ID,
            process.env.COLLECTION_ID,
            [Query.equal("date", DDMMYYYY)]
        );

        let documentId =
            searchResponse.documents.length > 0
                ? searchResponse.documents[0].$id
                : null;

        const cryptoRates = await fetchCryptoMarketData();

        const document = {
            date: DDMMYYYY,
            jsonRates: JSON.stringify(ratesArray),
            jsonCryptos: JSON.stringify(cryptoRates),
        };
        if (documentId) {
            console.log("Updating existing:", documentId, document);
            // Update the existing document
            await database.updateDocument(
                process.env.DATABASE_ID,
                process.env.COLLECTION_ID,
                documentId,
                document
            );
        } else {
            documentId = ID.unique();
            console.log("Saving new:", documentId, document);
            // Create a new document
            await database.createDocument(
                process.env.DATABASE_ID,
                process.env.COLLECTION_ID,
                documentId,
                document
            );
        }
        console.log("Done:", documentId);

        return context?.res
            ? context.res.json({ ok: true, rates: rates })
            : undefined;
    } catch (error) {
        console.error("Error fetching or saving rates:", error);
        return context?.res
            ? context.res.json({ ok: false, error: error })
            : undefined;
    }
}

async function fetchCryptoMarketData() {
    if (!process.env.CRYPTORANK_API_KEY) {
        console.log("CryptoRank key not set, skipping crypto fetch");
        return [];
    }

    const baseUrl = "https://api.cryptorank.io/v2";
    const headers = { "X-Api-Key": process.env.CRYPTORANK_API_KEY };

    const limit = 100;
    let skip = 0;
    const result = [];

    while (true) {
        const res = await fetch(
            `${baseUrl}/currencies?limit=${limit}&skip=${skip}`,
            { headers }
        );
        if (!res.ok) {
            console.error("CryptoRank currencies fetch failed", res.status);
            break;
        }
        const page = await res.json();
        const currencies = page.data || [];

        result.push(...currencies);
        if (currencies.length < limit) break;
        skip += limit;
    }

    return result;
}

// file: src/main.js
import { Client, Databases, Query, ID } from "node-appwrite";

export default async function fetchAndSaveRates(context) {
    const log = (...args) =>
        context?.log ? context.log(...args) : console.log(...args);
    const logError = (...args) =>
        context?.error ? context.error(...args) : console.error(...args);

    try {
        const client = new Client();
        const database = new Databases(client);
        client
            .setEndpoint("https://cloud.appwrite.io/v1")
            .setProject(process.env.PROJECT_ID)
            .setKey(process.env.APPWRITE_API_KEY);

        log(
            "Env vars set:",
            Boolean(process.env.PROJECT_ID),
            Boolean(process.env.DATABASE_ID),
            Boolean(process.env.COLLECTION_ID)
        );

        /* ---------- Fiat rates ---------- */
        const currencyResponse = await fetch(
            `https://api.currencyapi.com/v3/latest?apikey=${process.env.RATES_API_KEY}`
        );
        const currencyJson = await currencyResponse.json();
        const rates = currencyJson?.data ?? {};

        log("Retrieved rates", rates);

        const ratesArray = Object.entries(rates).map(([code, { value }]) => [
            code,
            value,
        ]);
        const DDMMYYYY = new Date()
            .toLocaleDateString("en-GB")
            .replace(/\//g, "");

        /* ---------- Crypto rates ---------- */
        const cryptoRates = await fetchCryptoMarketData(log, logError);

        /* ---------- Upsert document ---------- */
        const whereDate = [Query.equal("date", DDMMYYYY)];
        const existing = await database.listDocuments(
            process.env.DATABASE_ID,
            process.env.COLLECTION_ID,
            whereDate
        );
        const documentId =
            existing.total > 0 ? existing.documents[0].$id : ID.unique();

        const payload = {
            date: DDMMYYYY,
            jsonRates: JSON.stringify(ratesArray),
            jsonCryptos: JSON.stringify(cryptoRates),
        };

        if (existing.total > 0) {
            log("Updating:", documentId);
            await database.updateDocument(
                process.env.DATABASE_ID,
                process.env.COLLECTION_ID,
                documentId,
                payload
            );
        } else {
            log("Creating:", documentId);
            await database.createDocument(
                process.env.DATABASE_ID,
                process.env.COLLECTION_ID,
                documentId,
                payload
            );
        }

        log("Done:", documentId);
        return context?.res?.json({ ok: true, rates });
    } catch (err) {
        logError("Error fetching or saving rates:", err);
        return context?.res?.json({ ok: false, error: String(err) });
    }
}

async function fetchCryptoMarketData(
    log = console.log,
    logError = console.error
) {
    if (!process.env.CRYPTORANK_API_KEY) {
        log("CryptoRank key not set, skipping crypto fetch");
        return [];
    }

    const baseUrl = "https://api.cryptorank.io/v2";
    const headers = { "X-Api-Key": process.env.CRYPTORANK_API_KEY };

    const limit = 100;
    let skip = 0;
    const all = [];

    while (true) {
        const res = await fetch(
            `${baseUrl}/currencies?limit=${limit}&skip=${skip}`,
            { headers }
        );
        if (!res.ok) {
            logError("CryptoRank fetch failed:", res.status);
            break;
        }
        const { data = [] } = await res.json();
        all.push(...data);
        if (data.length < limit) break;
        skip += limit;
    }

    return all;
}

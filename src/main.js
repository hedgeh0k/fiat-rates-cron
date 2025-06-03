import { Client, Databases, Query, ID, Functions } from "node-appwrite";

async function fetchWithRetry(url, opts = {}, max = 3, log = console.log) {
    for (let attempt = 1; attempt <= max; ++attempt) {
        try {
            const res = await fetch(url, opts);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            return await res.json();
        } catch (e) {
            log(`⚠️  ${url} failed (attempt ${attempt}):`, e.message);
            if (attempt === max) throw e;
            await new Promise((r) => setTimeout(r, 2 ** attempt * 1_000));
        }
    }
}

/** --------------- main entry --------------- */
export default async function fetchAndSaveRates(context) {
    const log = (...a) => (context?.log ?? console.log)(...a);
    const logError = (...a) => (context?.error ?? console.error)(...a);

    try {
        const client = new Client()
            .setEndpoint("https://cloud.appwrite.io/v1")
            .setProject(process.env.PROJECT_ID)
            .setKey(process.env.APPWRITE_API_KEY);

        const db = new Databases(client);
        const functions = new Functions(client); // for e-mail alert

        const [fiatJson, cryptoRaw] = await Promise.all([
            fetchWithRetry(
                `https://api.currencyapi.com/v3/latest?type=fiat&apikey=${process.env.RATES_API_KEY}`,
                {},
                2,
                log
            ),
            fetchCryptoMarketData(log, logError), // see below
        ]);

        const fiatRates = Object.entries(fiatJson?.data ?? {}).map(
            ([code, { value }]) => [code, value]
        );
        const cryptoRates = cryptoRaw.map((c) => [c.symbol, c.price]);
        const cryptoMeta = Object.fromEntries(
            cryptoRaw.map((c) => [
                c.symbol,
                {
                    id: c.id,
                    name: c.name,
                    rank: c.rank,
                    type: c.type,
                    lastUpdated: c.lastUpdated,
                    totalSupply: c.totalSupply,
                    maxSupply: c.maxSupply,
                    circulatingSupply: c.circulatingSupply,
                    price: c.price,
                    high24h: c.high24h,
                    low24h: c.low24h,
                    volume24h: c.volume24h,
                    marketCap: c.marketCap,
                    ath: c.ath, // keeps date, value, % change
                    atl: c.atl,
                    images: c.images, // x60, x150, icon, native
                },
            ])
        );

        /* ----------- Sanity checks -------------- */
        const mustHaveFiat = ["USD", "EUR", "RUB"];
        const missingFiat = mustHaveFiat.filter(
            (c) => !fiatRates.some(([code]) => code === c)
        );
        const hasBTC = cryptoRates.some(([sym]) => sym === "BTC");

        if (missingFiat.length || !hasBTC) {
            throw new Error("Sanity check failed, aborting save");
        }

        /* ----------- Upsert in DB --------------- */
        const dateStr = new Date()
            .toLocaleDateString("en-GB")
            .replace(/\//g, "");
        const search = await db.listDocuments(
            process.env.DATABASE_ID,
            process.env.COLLECTION_ID,
            [Query.equal("date", dateStr)]
        );

        const docId = search.total ? search.documents[0].$id : ID.unique();
        const record = {
            date: dateStr,
            fiatRates: JSON.stringify(fiatRates),
            cryptoRates: JSON.stringify(cryptoRates),
            cryptoMeta: JSON.stringify(cryptoMeta),
        };

        if (search.total) {
            log("Updating doc", docId);
            await db.updateDocument(
                process.env.DATABASE_ID,
                process.env.COLLECTION_ID,
                docId,
                record
            );
        } else {
            log("Creating doc", docId);
            await db.createDocument(
                process.env.DATABASE_ID,
                process.env.COLLECTION_ID,
                docId,
                record
            );
        }

        log("✅ Rates stored", docId);
        return context?.res?.json({ ok: true });
    } catch (err) {
        logError("⛔ Cron failed:", err);
        return context?.res?.json({ ok: false, error: String(err) });
    }
}

async function fetchCryptoMarketData(
    log = console.log,
    logError = console.error
) {
    if (!process.env.CRYPTORANK_API_KEY) {
        log("CryptoRank key absent → skip crypto");
        return [];
    }
    const base = "https://api.cryptorank.io/v2/currencies";
    const headers = { "X-Api-Key": process.env.CRYPTORANK_API_KEY };
    const limit = 100;
    let skip = 0,
        out = [];

    while (true) {
        const { data = [] } = await fetchWithRetry(
            `${base}?limit=${limit}&skip=${skip}`,
            { headers },
            3,
            log
        );
        out.push(...data);
        if (data.length < limit) break;
        skip += limit;
    }
    return out;
}

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

        /* ----------- Upsert in new DBs --------------- */
        const dateStr = new Date()
            .toLocaleDateString("en-GB")
            .replace(/\//g, "");

        // Target: new rates database/collection
        const ratesDbId = process.env.RATES1_DATABASE_ID;
        const ratesCollectionId = process.env.RATES_COMBINED_COLLECTION_ID;
        const metaDbId = process.env.CRYPTOMETA_DATABASE_ID;
        const metaCollectionId = process.env.CRYPTOMETA_COLLECTION_ID;

        if (!ratesDbId || !ratesCollectionId)
            throw new Error(
                "Missing RATES1_DATABASE_ID or RATES_COMBINED_COLLECTION_ID"
            );
        if (!metaDbId || !metaCollectionId)
            throw new Error(
                "Missing CRYPTOMETA_DATABASE_ID or CRYPTOMETA_COLLECTION_ID"
            );

        // Upsert today's rates (no cryptoMeta here)
        const todaySearch = await db.listDocuments(
            ratesDbId,
            ratesCollectionId,
            [Query.equal("date", dateStr)]
        );
        const docId = todaySearch.total
            ? todaySearch.documents[0].$id
            : ID.unique();
        const ratesRecord = {
            date: dateStr,
            fiatRates: JSON.stringify(fiatRates),
            cryptoRates: JSON.stringify(cryptoRates),
        };

        if (todaySearch.total) {
            log("Updating rates doc (new DB)", docId);
            await db.updateDocument(
                ratesDbId,
                ratesCollectionId,
                docId,
                ratesRecord
            );
        } else {
            log("Creating rates doc (new DB)", docId);
            await db.createDocument(
                ratesDbId,
                ratesCollectionId,
                docId,
                ratesRecord
            );
        }

        // Upsert today's cryptoMeta into its dedicated database/collection
        if (cryptoRaw.length) {
            await upsertCryptoMetaDocument(
                db,
                metaDbId,
                metaCollectionId,
                dateStr,
                JSON.stringify(cryptoMeta),
                log
            );
        } else {
            log("No crypto meta fetched today — skipping cryptoMeta upsert.");
        }

        // Optional: full migration from old DB/collection → new DBs
        if (process.env.MIGRATE_TO_NEW_DB === "1") {
            await migrateFromOldToNewDatabases(
                db,
                {
                    oldDbId: process.env.DATABASE_ID,
                    oldCollectionId: process.env.COLLECTION_ID,
                },
                {
                    ratesDbId,
                    ratesCollectionId,
                },
                {
                    metaDbId,
                    metaCollectionId,
                },
                log,
                logError
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

// ---------- helpers: meta upsert + full-database migration ----------
async function upsertCryptoMetaDocument(
    db,
    databaseId,
    collectionId,
    dateStr,
    metaString,
    log
) {
    const metaDocId = dateStr; // per-day doc id
    const payload = {
        date: dateStr,
        cryptoMeta: metaString,
    };
    try {
        await db.createDocument(databaseId, collectionId, metaDocId, payload);
        log("Created cryptoMeta doc", metaDocId);
    } catch (e) {
        await db.updateDocument(databaseId, collectionId, metaDocId, payload);
        log("Updated cryptoMeta doc", metaDocId);
    }
}

async function migrateFromOldToNewDatabases(
    db,
    { oldDbId, oldCollectionId },
    { ratesDbId, ratesCollectionId },
    { metaDbId, metaCollectionId },
    log,
    logError
) {
    if (!oldDbId || !oldCollectionId) {
        log(
            "⚠️  MIGRATE_TO_NEW_DB=1 set, but old DATABASE_ID/COLLECTION_ID not provided — skipping migration."
        );
        return;
    }
    log("🔄 Migration start: old → new DBs");
    const pageSize = 100;
    let cursor = null;
    let scanned = 0,
        movedRates = 0,
        movedMeta = 0,
        strippedOld = 0;

    while (true) {
        const queries = [Query.orderDesc("$createdAt"), Query.limit(pageSize)];
        if (cursor) queries.push(Query.cursorAfter(cursor));
        const page = await db.listDocuments(oldDbId, oldCollectionId, queries);
        if (!page.documents.length) break;

        for (const doc of page.documents) {
            scanned++;
            const migratedDate = doc.date || doc.$id;
            const ratesRecord = {
                date: migratedDate,
                fiatRates:
                    typeof doc.fiatRates === "string"
                        ? doc.fiatRates
                        : JSON.stringify(doc.fiatRates ?? {}),
                cryptoRates:
                    typeof doc.cryptoRates === "string"
                        ? doc.cryptoRates
                        : JSON.stringify(doc.cryptoRates ?? []),
            };
            // Upsert rates in new DB
            try {
                const existing = await db.listDocuments(
                    ratesDbId,
                    ratesCollectionId,
                    [Query.equal("date", migratedDate)]
                );
                const rId = existing.total
                    ? existing.documents[0].$id
                    : ID.unique();
                if (existing.total) {
                    await db.updateDocument(
                        ratesDbId,
                        ratesCollectionId,
                        rId,
                        ratesRecord
                    );
                } else {
                    await db.createDocument(
                        ratesDbId,
                        ratesCollectionId,
                        rId,
                        ratesRecord
                    );
                }
                movedRates++;
            } catch (e) {
                logError(
                    "Rates upsert failed for",
                    migratedDate,
                    e?.message ?? e
                );
            }

            // Upsert meta in meta DB (if exists in old doc)
            if (doc.cryptoMeta && String(doc.cryptoMeta).length > 2) {
                try {
                    const metaString =
                        typeof doc.cryptoMeta === "string"
                            ? doc.cryptoMeta
                            : JSON.stringify(doc.cryptoMeta);
                    await upsertCryptoMetaDocument(
                        db,
                        metaDbId,
                        metaCollectionId,
                        migratedDate,
                        metaString,
                        log
                    );
                    movedMeta++;
                } catch (e) {
                    logError(
                        "Meta upsert failed for",
                        migratedDate,
                        e?.message ?? e
                    );
                }
                // Try to strip from old doc to lighten UI load
                try {
                    await db.updateDocument(oldDbId, oldCollectionId, doc.$id, {
                        cryptoMeta: null,
                    });
                    strippedOld++;
                } catch {
                    try {
                        await db.updateDocument(
                            oldDbId,
                            oldCollectionId,
                            doc.$id,
                            { cryptoMeta: "" }
                        );
                        strippedOld++;
                    } catch {}
                }
            }
        }
        cursor = page.documents[page.documents.length - 1].$id;
        if (page.documents.length < pageSize) break;
    }
    log(
        `🔚 Migration complete. scanned=${scanned}, ratesUpserted=${movedRates}, metaUpserted=${movedMeta}, oldStripped=${strippedOld}`
    );
}

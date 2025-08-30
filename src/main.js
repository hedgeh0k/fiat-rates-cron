import { Client, Databases, Query, ID, Functions } from "node-appwrite";

/* ===================== global crash handlers (so you see logs) ===================== */
process.on("unhandledRejection", (reason) => {
    console.error("[rates] UNHANDLED_REJECTION:", reason);
});
process.on("uncaughtException", (err) => {
    console.error("[rates] UNCAUGHT_EXCEPTION:", err);
});

/* ===================== HTTP fetch with retries & per-request timeout ===================== */
async function fetchWithRetry(
    url,
    opts = {},
    max = 3,
    log = console.log,
    timeoutMs = 10000
) {
    for (let attempt = 1; attempt <= max; ++attempt) {
        try {
            const controller = new AbortController();
            const t = setTimeout(
                () => controller.abort(new Error(`timeout ${timeoutMs}ms`)),
                timeoutMs
            );
            const res = await fetch(url, {
                ...opts,
                signal: controller.signal,
            });
            clearTimeout(t);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            return await res.json();
        } catch (e) {
            log(
                `[fetch] ${url} failed (attempt ${attempt}/${max}):`,
                e?.message ?? e
            );
            if (attempt === max) throw e;
            await new Promise((r) => setTimeout(r, 2 ** attempt * 1000));
        }
    }
}

/* ===================== tiny helpers ===================== */
function okRes(context, payload) {
    // Support both old/new function APIs
    if (context?.res?.json) return context.res.json(payload);
    return payload;
}
function errRes(context, message, extra = {}) {
    console.error("[rates] ERROR:", message, extra);
    if (context?.res?.json)
        return context.res.json({
            ok: false,
            error: String(message),
            ...extra,
        });
    return { ok: false, error: String(message), ...extra };
}

/* ===================== main entry ===================== */
export default async function fetchAndSaveRates(context) {
    const log = (...a) => console.log("[rates]", ...a);
    const logError = (...a) => console.error("[rates]", ...a);

    log("START execution");

    /* ---- ENV presence ---- */
    const {
        PROJECT_ID,
        APPWRITE_API_KEY,
        RATES_API_KEY,
        RATES1_DATABASE_ID,
        RATES_COMBINED_COLLECTION_ID,
        CRYPTORANK_API_KEY,
        CRYPTOMETA_DATABASE_ID,
        CRYPTOMETA_COLLECTION_ID,
        CRYPTORANK_MAX_PAGES,
        MIGRATE_TO_NEW_DB,
        DATABASE_ID,
        COLLECTION_ID,
    } = process.env;

    const missingEnv = [];
    if (!PROJECT_ID) missingEnv.push("PROJECT_ID");
    if (!APPWRITE_API_KEY) missingEnv.push("APPWRITE_API_KEY");
    if (!RATES_API_KEY) missingEnv.push("RATES_API_KEY");
    if (!RATES1_DATABASE_ID) missingEnv.push("RATES1_DATABASE_ID");
    if (!RATES_COMBINED_COLLECTION_ID)
        missingEnv.push("RATES_COMBINED_COLLECTION_ID");
    if (!CRYPTOMETA_DATABASE_ID) missingEnv.push("CRYPTOMETA_DATABASE_ID");
    if (!CRYPTOMETA_COLLECTION_ID) missingEnv.push("CRYPTOMETA_COLLECTION_ID");

    if (missingEnv.length) {
        return errRes(context, "Missing required env vars", { missingEnv });
    }

    try {
        /* ---- Appwrite clients ---- */
        const client = new Client()
            .setEndpoint("https://cloud.appwrite.io/v1")
            .setProject(PROJECT_ID)
            .setKey(APPWRITE_API_KEY);
        const db = new Databases(client);
        // eslint-disable-next-line no-unused-vars
        const functions = new Functions(client);
        log("Appwrite SDK initialized");

        /* ---- Fetch data (with clear logs per step) ---- */
        log("Fetching fiat & crypto in parallel...");
        const fiatPromise = fetchWithRetry(
            `https://api.currencyapi.com/v3/latest?type=fiat&apikey=${RATES_API_KEY}`,
            {},
            2,
            (m, ...rest) => console.log("[fiat]", m, ...rest),
            10000
        );
        const cryptoPromise = fetchCryptoMarketData(
            (m, ...rest) => console.log("[crypto]", m, ...rest),
            (m, ...rest) => console.error("[crypto]", m, ...rest),
            CRYPTORANK_API_KEY,
            CRYPTORANK_MAX_PAGES
        );

        const [fiatRes, cryptoRes] = await Promise.allSettled([
            fiatPromise,
            cryptoPromise,
        ]);

        if (fiatRes.status !== "fulfilled") {
            return errRes(context, "Fiat fetch failed", {
                reason: String(fiatRes.reason?.message ?? fiatRes.reason),
            });
        }
        if (cryptoRes.status !== "fulfilled") {
            // Continue without crypto if you prefer; here we fail loudly so you notice
            return errRes(context, "Crypto fetch failed", {
                reason: String(cryptoRes.reason?.message ?? cryptoRes.reason),
            });
        }

        const fiatJson = fiatRes.value;
        const cryptoRaw = cryptoRes.value;

        log(
            `Fetched fiat=${Object.keys(fiatJson?.data ?? {}).length} codes, crypto=${cryptoRaw.length} rows`
        );

        /* ---- Transform ---- */
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
        log("Sanity check:", { missingFiat, hasBTC });

        if (missingFiat.length || !hasBTC) {
            return errRes(context, "Sanity check failed", {
                missingFiat,
                hasBTC,
            });
        }

        /* ----------- Upsert in new DBs --------------- */
        const dateStr = new Date()
            .toLocaleDateString("en-GB")
            .replace(/\//g, "");
        log("Using date key:", dateStr);

        /* ---- Upsert rates (primary collection) ---- */
        const ratesRecord = {
            date: dateStr,
            fiatRates: JSON.stringify(fiatRates),
            cryptoRates: JSON.stringify(cryptoRates),
        };

        log("Upserting rates (query by date)...");
        let ratesDocId = null;
        try {
            const todaySearch = await db.listDocuments(
                RATES1_DATABASE_ID,
                RATES_COMBINED_COLLECTION_ID,
                [Query.equal("date", dateStr)]
            );
            ratesDocId = todaySearch.total
                ? todaySearch.documents[0].$id
                : ID.unique();

            if (todaySearch.total) {
                log("Updating rates doc", ratesDocId);
                await db.updateDocument(
                    RATES1_DATABASE_ID,
                    RATES_COMBINED_COLLECTION_ID,
                    ratesDocId,
                    ratesRecord
                );
            } else {
                log("Creating rates doc", ratesDocId);
                await db.createDocument(
                    RATES1_DATABASE_ID,
                    RATES_COMBINED_COLLECTION_ID,
                    ratesDocId,
                    ratesRecord
                );
            }
        } catch (e) {
            // If schema/index is missing (e.g., "Attribute not found in schema: date")
            const msg = String(e?.message ?? e);
            logError(
                "Rates upsert via query failed, will fallback to ID:",
                msg
            );
            ratesDocId = dateStr; // stable ID for today
            try {
                await db.createDocument(
                    RATES1_DATABASE_ID,
                    RATES_COMBINED_COLLECTION_ID,
                    ratesDocId,
                    ratesRecord
                );
                log("Created rates doc (by ID)", ratesDocId);
            } catch (e2) {
                // If already exists: update
                try {
                    await db.updateDocument(
                        RATES1_DATABASE_ID,
                        RATES_COMBINED_COLLECTION_ID,
                        ratesDocId,
                        ratesRecord
                    );
                    log("Updated rates doc (by ID)", ratesDocId);
                } catch (e3) {
                    return errRes(context, "Rates upsert failed", {
                        e: String(e3?.message ?? e3),
                    });
                }
            }
        }

        /* ---- Upsert today's cryptoMeta (separate DB/collection) ---- */
        if (cryptoRaw.length) {
            const metaPayload = {
                date: dateStr,
                cryptoMeta: JSON.stringify(cryptoMeta),
            };
            const metaDocId = dateStr; // per-day doc id
            log("Upserting cryptoMeta doc", metaDocId);
            try {
                await db.createDocument(
                    CRYPTOMETA_DATABASE_ID,
                    CRYPTOMETA_COLLECTION_ID,
                    metaDocId,
                    metaPayload
                );
                log("Created cryptoMeta", metaDocId);
            } catch (e) {
                try {
                    await db.updateDocument(
                        CRYPTOMETA_DATABASE_ID,
                        CRYPTOMETA_COLLECTION_ID,
                        metaDocId,
                        metaPayload
                    );
                    log("Updated cryptoMeta", metaDocId);
                } catch (e2) {
                    return errRes(context, "cryptoMeta upsert failed", {
                        e: String(e2?.message ?? e2),
                    });
                }
            }
        } else {
            log("No crypto meta today — skipping meta upsert");
        }

        /* ---- Optional migration: RATES ONLY ---- */
        if (MIGRATE_TO_NEW_DB === "1") {
            log("Starting migration of old rates → new rates...");
            try {
                await migrateFromOldToNewDatabases(
                    db,
                    { oldDbId: DATABASE_ID, oldCollectionId: COLLECTION_ID },
                    {
                        ratesDbId: RATES1_DATABASE_ID,
                        ratesCollectionId: RATES_COMBINED_COLLECTION_ID,
                    },
                    {
                        metaDbId: CRYPTOMETA_DATABASE_ID,
                        metaCollectionId: CRYPTOMETA_COLLECTION_ID,
                    },
                    log,
                    logError
                );
                log("Migration finished");
            } catch (e) {
                logError("Migration crashed:", e?.message ?? e);
            }
        }

        log("DONE. ratesDocId:", ratesDocId);
        return okRes(context, { ok: true, ratesDocId });
    } catch (err) {
        // Any error that escaped the above will be logged here
        logError("FATAL:", err?.message ?? err, err?.stack);
        return errRes(context, "Fatal error", { stack: err?.stack });
    }
}

/* ===================== Crypto fetch: paginated + timeouts + cap ===================== */
async function fetchCryptoMarketData(
    log = console.log,
    logError = console.error,
    apiKey,
    maxPagesEnv
) {
    if (!apiKey) {
        log("CryptoRank key absent → skip crypto");
        return [];
    }
    const base = "https://api.cryptorank.io/v2/currencies";
    const headers = { "X-Api-Key": apiKey };
    const limit = 100;
    const maxPages = Math.max(1, Number(maxPagesEnv ?? 10)); // default cap: 1000 rows
    let skip = 0;
    let out = [];
    let pageCount = 0;

    while (true) {
        pageCount++;
        const url = `${base}?limit=${limit}&skip=${skip}`;
        log(`GET ${url} (page ${pageCount}/${maxPages})`);
        const { data = [] } = await fetchWithRetry(
            url,
            { headers },
            3,
            log,
            10000
        );
        out.push(...data);
        log(`Fetched ${data.length} rows; total=${out.length}`);
        if (data.length < limit) break;
        if (pageCount >= maxPages) {
            log(
                `Page cap ${maxPages} reached; stopping pagination to prevent timeout`
            );
            break;
        }
        skip += limit;
    }
    return out;
}

/* ===================== DB helpers ===================== */
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

// RATES-ONLY migration. Leaves cryptoMeta alone.
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
    log("🔄 Migration start: old → new DBs (rates only)");
    const pageSize = 100;
    let cursor = null;
    let scanned = 0,
        movedRates = 0;

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
        }

        cursor = page.documents[page.documents.length - 1].$id;
        if (page.documents.length < pageSize) break;
    }
    log(
        `🔚 Migration complete. scanned=${scanned}, ratesUpserted=${movedRates}`
    );
}

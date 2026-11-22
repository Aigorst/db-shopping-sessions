# Shopping Sessions: native C++ pipeline

One statically linked C++17 binary (`src/main.cpp`) in a `FROM scratch` image. No engine, no
libraries: the CSV scanner, timestamp parser, hash tables, and Parquet writer are all here.

## Pipeline

1. **Setup.** Read `manifest.json`, `products.csv` and `campaigns.csv`, and mmap every CSV with a
   zero page after it so SIMD loads may overrun. Files are cut into 4 MB byte ranges; each worker
   aligns its range to lines itself.
2. **Parse (8 threads, work queue).**
   - Each 64-byte block yields bitmasks for `,`, `\n` and `"` (AVX2 / NEON).
   - Fields are cut from the comma offsets, and quote stripping is branch-free: a line is valid
     iff each quoted field is wrapped whole and the line's quote count is twice the number of
     quoted fields.
   - Timestamps use SWAR digit validation and full calendar checks.
   - IDs of the shape `[byte][0-9a-z]{0,8}` pack injectively into 64 bits (6 bits per char).
     Others are interned. Byte-wise order is kept: same prefix and length compare numerically,
     anything else decodes and compares.
   - Events are scattered by `hash(event_id)` into per-thread block lists.
   - Clicks are kept whole: copies are identical, so dedup can't change credit or assists.
3. **Links (overlapped).** Link chunks are parsed first. The thread that finishes the last
   chunk runs union-find and builds `id -> person` (the byte-wise smallest ID), while the
   others keep parsing events.
4. **Dedup (per event bucket).** A cache-resident open-addressing table keeps the copy with the
   minimum `ingest_ts`, and the kept copy is validated (validity was computed at parse time).
   Survivors get their person (lookups prefetched) and are scattered by `hash(person)`.
5. **Sessions (per person bucket).**
   - Group by person, then radix-sort by `ts` (stable counting placement keeps each person's
     events in time order).
   - Sessionize, credit the session with a two-pointer 7-day click window, and count assists
     as activity spans resolved per day at the end.
6. **Output.** A hand-written Parquet writer: PLAIN, uncompressed, one data page per column,
   `DATE` as INT32 with its logical type. One `user_daily` file per thread.

## Checking

- `tools/reference.py` is a slow, literal implementation of `rules.md` for cross-checking.
- `make check` builds natively and runs the starter fixtures plus the dev comparison
  (paths in the Makefile).

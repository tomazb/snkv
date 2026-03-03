# Proposal: Iterator Bounds and Reverse Iteration for SNKV

**Status:** RFC (Request for Comments)
**Author:** SNKV maintainers
**Created:** 2026-03-03
**Target version:** 0.4.0

---

## Table of Contents

- [Motivation](#motivation)
- [Goals](#goals)
- [Non-Goals](#non-goals)
- [Background](#background)
- [Design](#design)
  - [KVIterator struct changes](#kviterator-struct-changes)
  - [API](#api)
  - [Internals: Reverse Iteration](#internals-reverse-iteration)
  - [Internals: Bounds](#internals-bounds)
  - [Internals: TTL interaction](#internals-ttl-interaction)
  - [Column Family Support](#column-family-support)
- [Performance Analysis](#performance-analysis)
- [Error Handling](#error-handling)
- [Backward Compatibility](#backward-compatibility)
- [Test Plan](#test-plan)
- [Python API](#python-api)
- [Alternatives Considered](#alternatives-considered)
- [Open Questions](#open-questions)
- [Implementation Checklist](#implementation-checklist)

---

## Motivation

SNKV's current iterator supports forward-only traversal starting from the first
key. This blocks two common and fundamental query patterns:

**Reverse iteration — no workaround:**
- "Latest N events" — must read all keys into memory and reverse in the caller.
- Leaderboard (top scores descending) — O(n) full scan instead of O(k).
- Time-series descending — the dominant access pattern for logs and metrics.
- Cursor-based pagination backward through results.

**Bounded range scans — partial workaround only:**
- Bounded range queries `[A, B]`: the existing iterator has no upper bound;
  callers must manually compare each key in the loop and break.
- The prefix iterator covers only the "starts with X" case. Arbitrary range
  queries with a stop key are not natively supported.

These two features together close the remaining iterator capability gap without
any architectural changes to SNKV.

---

## Goals

1. Reverse iteration — iterate keys from last to first, or backward to a lower
   bound.
2. Upper bound — stop a forward iterator automatically when the current key
   exceeds the bound. No manual key comparison needed in the caller.
3. Lower bound — stop a reverse iterator automatically when the current key
   falls below the bound.
4. Works with column families — all new functions have default-CF and named-CF
   variants consistent with the existing API.
5. TTL-aware — reverse iteration correctly skips expired keys, same as forward
   iteration today.
6. Backward compatible — all existing iterator behaviour is unchanged. New
   fields on `KVIterator` are zero-initialised and inactive unless explicitly set.

---

## Non-Goals

- Bidirectional switching — an iterator created for forward traversal cannot be
  switched to reverse mid-scan. Direction is set at creation time.
- Prefix reverse iteration — reverse iteration with a prefix filter is not
  included. Use lower bound + upper bound instead.
- Iterator invalidation on write — same as existing: the iterator cursor is
  marked `CURSOR_REQUIRESSEEK` by the btree layer on write and re-seeks
  automatically on the next step. No new semantics.
- Count / aggregate operations — not in scope.

---

## Background

### Existing iterator internals

`KVIterator` (lines 232–244 of `kvstore.c`) holds a `BtCursor *pCur` pointing
into the B-tree for the target CF. The current implementation uses:

```
sqlite3BtreeFirst(pCur, &res)   — position at smallest key
sqlite3BtreeNext(pCur, 0)       — advance forward
sqlite3BtreeIndexMoveto(pCur, pIdxKey, &res) — seek to key (used in prefix iterator)
```

The SQLite B-tree already provides the symmetric reverse primitives, declared in
`include/btree.h` and implemented in `src/btree.c`:

```
sqlite3BtreeLast(pCur, &res)        — position at largest key
sqlite3BtreePrevious(pCur, flags)   — advance backward
```

Neither `sqlite3BtreeLast` nor `sqlite3BtreePrevious` is currently called from
`kvstore.c`. No new btree primitives are needed — this proposal wires them up.

A new `kvstoreSeekBefore` static helper (first key strictly < target) is needed
for TTL lazy-delete recovery under reverse iteration. It mirrors the existing
`kvstoreSeekAfter` helper (line 396).

---

## Design

### KVIterator struct changes

Three fields are added to `KVIterator` (after the existing `pPrefix`/`nPrefix`):

```c
struct KVIterator {
  KVColumnFamily *pCF;
  BtCursor *pCur;
  int eof;
  int ownsTrans;
  void *pKeyBuf;   int nKeyBuf;
  void *pValBuf;   int nValBuf;
  int isValid;
  void *pPrefix;   int nPrefix;

  /* --- new fields --- */
  int reverse;              /* 1 = iterate backward (BtreeLast/BtreePrevious) */
  void *pUpperBound;        /* inclusive upper bound; NULL = no upper limit */
  int nUpperBound;
  void *pLowerBound;        /* inclusive lower bound; NULL = no lower limit */
  int nLowerBound;
};
```

All three fields are zero-initialised by `sqlite3MallocZero` in
`kvstore_cf_iterator_create`. An iterator with `reverse=0`, `pUpperBound=NULL`,
`pLowerBound=NULL` behaves identically to the current implementation — no
existing behaviour changes.

---

### API

#### Reverse iterator creation

```c
/*
** Create a reverse iterator for the default column family.
**
** A reverse iterator starts positioned before the last entry.
** Call kvstore_iterator_last() to move to the first (largest) key,
** then kvstore_iterator_prev() to advance backward.
**
** Returns:
**   KVSTORE_OK on success, error code otherwise.
*/
int kvstore_reverse_iterator_create(KVStore *pKV, KVIterator **ppIter);

/*
** CF variant of kvstore_reverse_iterator_create.
*/
int kvstore_cf_reverse_iterator_create(KVColumnFamily *pCF, KVIterator **ppIter);
```

#### Reverse movement

```c
/*
** Position a reverse iterator at the last (largest) key.
**
** Equivalent to kvstore_iterator_first() for forward iterators.
** Must be called on an iterator created with kvstore_reverse_iterator_create()
** or kvstore_cf_reverse_iterator_create().
**
** If a lower bound is set, positions at the largest key >= lower bound.
** If an upper bound is set, positions at the last key <= upper bound.
**
** Returns:
**   KVSTORE_OK on success (check kvstore_iterator_eof for emptiness).
*/
int kvstore_iterator_last(KVIterator *pIter);

/*
** Advance a reverse iterator to the previous (smaller) key.
**
** Equivalent to kvstore_iterator_next() for forward iterators.
** Sets eof=1 when the beginning (or lower bound) is reached.
**
** Returns:
**   KVSTORE_OK on success.
*/
int kvstore_iterator_prev(KVIterator *pIter);
```

#### Bounds

```c
/*
** Set an inclusive upper bound on a forward iterator.
**
** When the current key exceeds pKey, kvstore_iterator_next() sets eof=1
** and kvstore_iterator_eof() returns 1. The bound is checked after each
** step, including after the initial first().
**
** pKey bytes are copied; the caller's buffer may be freed immediately.
** Must be called before kvstore_iterator_first().
**
** Returns:
**   KVSTORE_OK on success.
**   KVSTORE_NOMEM if the bound could not be copied.
*/
int kvstore_iterator_set_upper_bound(
  KVIterator *pIter,
  const void *pKey, int nKey
);

/*
** Set an inclusive lower bound on a reverse iterator.
**
** When the current key falls below pKey, kvstore_iterator_prev() sets eof=1.
** Must be called before kvstore_iterator_last().
**
** Returns:
**   KVSTORE_OK on success.
**   KVSTORE_NOMEM if the bound could not be copied.
*/
int kvstore_iterator_set_lower_bound(
  KVIterator *pIter,
  const void *pKey, int nKey
);
```

---

### Internals: Reverse Iteration

`kvstore_reverse_iterator_create` calls `kvstore_cf_iterator_create` and sets
`pIter->reverse = 1`. No other changes to the creation path.

`kvstore_iterator_last`:

```
1. If pIter->pUpperBound:
     Seek to pUpperBound using sqlite3BtreeIndexMoveto
     If res > 0 (cursor past bound): back up with sqlite3BtreePrevious
     If eof: set pIter->eof = 1, return
2. Else:
     sqlite3BtreeLast(pIter->pCur, &res)
     pIter->eof = res
3. If !eof: check lower bound — if current key < lower bound, set eof = 1
4. If !eof: kvstoreIterSkipExpiredReverse(pIter)
```

`kvstore_iterator_prev`:

```
1. If pIter->eof: return KVSTORE_OK
2. sqlite3BtreePrevious(pIter->pCur, 0)
3. If SQLITE_DONE: pIter->eof = 1, return KVSTORE_OK
4. Check lower bound: if current key < lower bound, pIter->eof = 1, return
5. kvstoreIterSkipExpiredReverse(pIter)
```

---

### Internals: Bounds

Bound checking is a byte comparison using the existing
`sqlite3BtreePayload` + `memcmp` pattern already used by
`kvstoreIterCheckPrefix`. A new static helper `kvstoreIterCheckBound` extracts
the current key from the cursor and compares it against the bound:

```c
/*
** Returns:
**  -1  current key < bound
**   0  current key == bound
**  +1  current key > bound
*/
static int kvstoreIterCmpBound(KVIterator *pIter, const void *pBound, int nBound);
```

`iterator_next` adds at step 4 (after `BtreeNext` succeeds):
```
if( pIter->pUpperBound ){
  int cmp = kvstoreIterCmpBound(pIter, pIter->pUpperBound, pIter->nUpperBound);
  if( cmp > 0 ){ pIter->eof = 1; return KVSTORE_OK; }
}
```

`iterator_prev` adds at step 4 (after `BtreePrevious` succeeds):
```
if( pIter->pLowerBound ){
  int cmp = kvstoreIterCmpBound(pIter, pIter->pLowerBound, pIter->nLowerBound);
  if( cmp < 0 ){ pIter->eof = 1; return KVSTORE_OK; }
}
```

`kvstore_iterator_close` is extended to free `pUpperBound` and `pLowerBound`
alongside the existing `pPrefix` free.

---

### Internals: TTL interaction

`kvstoreIterSkipExpired` (line 2835) handles forward iteration lazy-delete
recovery: after deleting an expired key, it calls `kvstoreSeekAfter` to
reposition the cursor at the first entry strictly > the deleted key.

For reverse iteration, a new static helper `kvstoreIterSkipExpiredReverse` is
needed. It is structurally identical to `kvstoreIterSkipExpired` with two
differences:

1. After the lazy delete, it calls a new `kvstoreSeekBefore` helper (first
   entry strictly < deleted key) instead of `kvstoreSeekAfter`.
2. After repositioning, it checks the **lower bound** instead of `pPrefix`.

**`kvstoreSeekBefore`** (new static, mirrors `kvstoreSeekAfter` at line 396):

```c
/* Position pCur at the last entry strictly less than (pKey, nKey).
** Sets *pEof = 1 if no such entry exists.
*/
static int kvstoreSeekBefore(
  BtCursor *pCur,
  KeyInfo *pKeyInfo,
  const void *pKey, int nKey,
  int *pEof
){
  UnpackedRecord idxKey; Mem memField;
  int res = 0, rc;
  /* ... same UnpackedRecord setup ... */
  rc = sqlite3BtreeIndexMoveto(pCur, &idxKey, &res);
  if( rc != SQLITE_OK ){ *pEof = 1; return rc; }
  if( res >= 0 ){
    /* At or after target — back up one entry. */
    rc = sqlite3BtreePrevious(pCur, 0);
    if( rc == SQLITE_DONE ){ *pEof = 1; return SQLITE_OK; }
    if( rc != SQLITE_OK ){ *pEof = 1; return rc; }
  }
  /* res < 0: cursor is already at the last entry < target. */
  *pEof = sqlite3BtreeEof(pCur);
  return SQLITE_OK;
}
```

---

### Column Family Support

All new functions have default-CF and CF-level variants following the existing
naming convention:

| Default CF | Named CF |
|---|---|
| `kvstore_reverse_iterator_create` | `kvstore_cf_reverse_iterator_create` |
| `kvstore_iterator_last` | (operates on the iterator handle — CF-agnostic) |
| `kvstore_iterator_prev` | (operates on the iterator handle — CF-agnostic) |
| `kvstore_iterator_set_upper_bound` | (operates on the iterator handle — CF-agnostic) |
| `kvstore_iterator_set_lower_bound` | (operates on the iterator handle — CF-agnostic) |

`kvstore_iterator_last`, `prev`, and bound setters operate on a `KVIterator *`
handle which already contains its CF — no CF parameter is needed.

---

## Performance Analysis

### Reverse iteration

`sqlite3BtreeLast` and `sqlite3BtreePrevious` are symmetric to `sqlite3BtreeFirst`
and `sqlite3BtreeNext` at the btree level. B-trees are bidirectional by design;
the page structure supports both directions with identical page-access cost.
Each `prev` call is O(1) amortised, same as `next`.

### Bounds

Each bound check is one call to `kvstoreIterCmpBound`: read the key length from
the 4-byte payload header, then `memcmp` the key bytes against the bound. Cost
is O(k) where k is the key length. For typical keys (< 256 bytes) this is
negligible compared to the B-tree traversal.

### Impact on existing (unbounded, forward) iterators

The `reverse`, `pUpperBound`, and `pLowerBound` fields are zero-initialised.
Every new check is guarded:

```c
if( pIter->pUpperBound ){  /* only if bound is set */
```

Zero overhead for all existing iterator usage.

---

## Error Handling

| Condition | Behaviour |
|---|---|
| `kvstore_iterator_last` on forward iterator | `KVSTORE_ERROR` — direction mismatch |
| `kvstore_iterator_prev` on forward iterator | `KVSTORE_ERROR` — direction mismatch |
| `kvstore_iterator_set_upper_bound` after first/last called | `KVSTORE_ERROR` — bound must be set before iteration starts |
| `kvstore_iterator_set_lower_bound` after first/last called | `KVSTORE_ERROR` — bound must be set before iteration starts |
| `BtreePrevious` returns `SQLITE_DONE` | `pIter->eof = 1`, `KVSTORE_OK` returned |
| Allocation failure in bound set | Returns `KVSTORE_NOMEM`; iterator usable without bound |
| DB corruption mid-iteration | Returns `KVSTORE_CORRUPT`, same as existing |

---

## Backward Compatibility

- No existing function signatures change.
- `KVIterator` is an opaque type in `kvstore.h` — adding fields is ABI-safe
  for all callers who allocate iterators through `kvstore_iterator_create`.
- `reverse=0`, `pUpperBound=NULL`, `pLowerBound=NULL` produce identical
  behaviour to the current implementation. Zero new overhead.
- `kvstore_iterator_first` and `kvstore_iterator_next` are unchanged.
- `kvstore_iterator_close` gains two extra `sqlite3_free` calls for bound
  buffers; these are no-ops when bounds are not set.

---

## Test Plan

### Unit tests (`tests/test_iterator_reverse.c`)

| # | Test | Verifies |
|---|---|---|
| 1 | `rev_basic` | Insert 10 keys → reverse iter → keys in descending order |
| 2 | `rev_empty` | Reverse iter on empty CF → eof immediately |
| 3 | `rev_single` | Reverse iter on single key → one result, then eof |
| 4 | `rev_last_first` | `iterator_last` + `iterator_prev` repeatedly → all keys |
| 5 | `rev_lower_bound` | Reverse iter with lower bound → stops at bound, inclusive |
| 6 | `rev_lower_bound_miss` | Lower bound > all keys → eof immediately |
| 7 | `fwd_upper_bound` | Forward iter + upper bound → stops at bound, inclusive |
| 8 | `fwd_upper_bound_miss` | Upper bound < first key → eof immediately |
| 9 | `rev_cf` | Reverse iterator on named CF |
| 10 | `rev_multi_cf` | Two CFs, independent reverse iterators |
| 11 | `rev_ttl_skip` | Reverse iter over CF with expired keys → expired keys skipped |
| 12 | `direction_mismatch` | `iterator_last` on forward iter → KVSTORE_ERROR |
| 13 | `bound_after_start` | set_upper_bound after iterator_first → KVSTORE_ERROR |
| 14 | `rev_with_fwd_prefix` | Confirm prefix iterator unaffected by new fields |

### Integration tests

- **"Latest N" query**: 1 million keys, fetch last 10 — verify O(10) behaviour
  (no full scan).
- **Pagination correctness**: paginate forward through 10,000 keys, then backward
  through same 10,000 — verify identical key set in reverse order.
- **Bounded range**: insert keys `a`–`z`, range query `[f, r]` — verify exactly
  `f` through `r` returned.
- **Python API**: end-to-end test covering all new iterator methods.

---

## Python API

The Python `snkv` package exposes the new iterator capabilities through the
existing `KVStore.iterator()` factory method extended with parameters:

```python
class KVStore:
    def iterator(
        self,
        *,
        reverse: bool = False,
        lower_bound: bytes | None = None,
        upper_bound: bytes | None = None,
    ) -> "Iterator":
        """
        Return an iterator over the default column family.

        reverse=True  — iterate from last key to first.
        lower_bound   — inclusive lower bound (reverse scan stops here).
        upper_bound   — inclusive upper bound (forward scan stops here).

        Usage (forward with upper bound):
            with db.iterator(upper_bound=b"user:200") as it:
                it.first()
                while not it.eof():
                    key, val = it.key(), it.value()
                    it.next()

        Usage (reverse / latest-N):
            with db.iterator(reverse=True) as it:
                it.last()
                for _ in range(10):
                    if it.eof(): break
                    print(it.key(), it.value())
                    it.prev()

        Usage (reverse with lower bound):
            with db.iterator(reverse=True, lower_bound=b"event:100") as it:
                it.last()
                while not it.eof():
                    key, val = it.key(), it.value()
                    it.prev()
        """
        ...

    def cf_iterator(
        self,
        cf: "ColumnFamily",
        *,
        reverse: bool = False,
        lower_bound: bytes | None = None,
        upper_bound: bytes | None = None,
    ) -> "Iterator": ...
```

```python
class Iterator:
    """Returned by KVStore.iterator(). Use as context manager."""
    def first(self) -> None: ...          # forward: move to first key
    def last(self) -> None: ...           # reverse: move to last key
    def next(self) -> None: ...           # forward: advance
    def prev(self) -> None: ...           # reverse: advance
    def eof(self) -> bool: ...
    def key(self) -> bytes: ...
    def value(self) -> bytes: ...
    def __enter__(self) -> "Iterator": ...
    def __exit__(self, *_) -> None: ...   # calls close()
    def close(self) -> None: ...
```

Bounds are passed at construction time (to `iterator()`), not set on the
iterator object after creation. This prevents the error case of setting a bound
after iteration has started.

### Common patterns

```python
# Forward scan with upper bound
with db.iterator(upper_bound=b"user:200") as it:
    it.first()
    while not it.eof():
        process(it.key(), it.value())
        it.next()

# Latest 10 events (reverse)
results = []
with db.iterator(reverse=True) as it:
    it.last()
    while not it.eof() and len(results) < 10:
        results.append((it.key(), it.value()))
        it.prev()

# Reverse scan with lower bound
with db.iterator(reverse=True, lower_bound=b"event:50") as it:
    it.last()
    while not it.eof():
        process(it.key(), it.value())
        it.prev()
```

---

## Alternatives Considered

### Alternative 1: Bidirectional iterator (switch direction mid-scan)

Allow `next` and `prev` on the same iterator. RocksDB supports this.

**Rejected because:**
- Adds `reverse` state change to `iterator_next` and `iterator_prev` — every
  call must check whether the direction has changed since the last step.
- TTL skip-expired recovery must handle both directions on the same iterator.
- Doubles the number of code paths in the hot path.
- Real-world usage almost never switches direction mid-scan. The simpler
  "direction set at creation" model covers all practical cases.

### Alternative 2: `ReadOptions` struct (RocksDB style)

Pass a `KVIteratorOptions` struct to the create function:

```c
KVIteratorOptions opts = {0};
opts.reverse = 1;
opts.lower_bound = ...;
kvstore_cf_iterator_create_v2(pCF, &opts, ppIter);
```

**Considered but deferred:** The current proposal sets bounds via separate
functions after creation. Either approach is equivalent. The separate-function
approach is more consistent with SNKV's existing style (no options structs).
A `v2` create function can be added later if needed.

### Alternative 3: Separate reverse-iterator type

Create `KVReverseIterator` as a distinct type.

**Rejected because:**
- Doubles all iterator-related code (close, key, value, TTL skip, CF ref).
- `KVIterator` already has all the state needed; a single `reverse` flag is
  sufficient to distinguish behaviour.

---

## Open Questions

**Q1. Should bounds be inclusive or exclusive?**

Current proposal: both bounds are inclusive (`[lower, upper]`). RocksDB uses
inclusive lower + exclusive upper (`[lower, upper)`), which makes ranges
composable: the upper bound of one page equals the lower bound of the next.
Should SNKV match RocksDB's convention?

**Q2. Should `kvstore_iterator_last` on a forward iterator return
`KVSTORE_ERROR` or silently work (position at last key)?**

Current proposal: `KVSTORE_ERROR` — direction mismatch is likely a bug. An
alternative: allow it to work, since the btree supports it. Explicit error is
safer.

---

## Implementation Checklist

- [ ] Add `reverse`, `pUpperBound`/`nUpperBound`, `pLowerBound`/`nLowerBound` to `KVIterator` struct
- [ ] Add `kvstoreSeekBefore` static helper (mirrors `kvstoreSeekAfter`)
- [ ] Add `kvstoreIterCmpBound` static helper (key vs bound comparison)
- [ ] Add `kvstoreIterSkipExpiredReverse` static helper (mirrors `kvstoreIterSkipExpired`)
- [ ] Implement `kvstore_cf_reverse_iterator_create` / `kvstore_reverse_iterator_create`
- [ ] Implement `kvstore_iterator_last`
- [ ] Implement `kvstore_iterator_prev`
- [ ] Implement `kvstore_iterator_set_upper_bound` / `kvstore_iterator_set_lower_bound`
- [ ] Add bound check to `kvstore_iterator_next` (upper bound)
- [ ] Add bound check to `kvstore_iterator_prev` (lower bound)
- [ ] Extend `kvstore_iterator_close` to free `pUpperBound` and `pLowerBound`
- [ ] Add all new declarations to `include/kvstore.h`
- [ ] Write `tests/test_iterator_reverse.c` (14 tests)
- [ ] Add `tests/test_iterator_reverse.c` to `Makefile` TEST_SRC
- [ ] Implement Python bindings in `python/snkv_module.c`
- [ ] Add `iterator()` parameter extensions to `python/snkv/__init__.py`
- [ ] Update `docs/api/API_SPECIFICATION.md` with new iterator section
- [ ] Update `docs/python_api/API.md` with Python iterator section
- [ ] Add `examples/iterator_reverse.c` and `python/examples/iterator.py`
- [ ] Run full test suite: all existing tests pass, 14 new tests pass
- [ ] Valgrind clean on `tests/test_iterator_reverse`

---

*Feedback welcome via GitHub Discussions or as comments on the PR that introduced this file.*

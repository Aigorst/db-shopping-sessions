// Shopping Sessions pipeline: one static binary, no runtime dependencies.
// Phases: links (union-find) || parse events+clicks -> dedup by event_id
// -> group by person -> sessionize/attribute/aggregate -> Parquet.
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <dirent.h>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int64_t i64;
typedef int32_t i32;

static const i64 DAY_MS = 86400000LL;
static const i64 GAP_MS = 1800000LL;
static const i64 WEEK_MS = 604800000LL;
static const u32 BAD_TS = 0xFFFFFFFFu;
static const u64 CLMARK = 0xFULL << 60;  // inline long click ids (see CRec)
static const u32 CAMP_MASK = 0xFFFFF;
static bool g_inline_clicks = true;

#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)

#ifdef SS_PGO_GEN
extern "C" void __gcov_dump(void);
#endif
static void die(const char* msg) {
  fprintf(stderr, "fatal: %s\n", msg);
  _exit(1);
}

// Bucket choice: top bits of hb(x). Table slots: top bits of ht(x). One multiply each.
static inline u64 hb(u64 x) { return x * 0x9E3779B97F4A7C15ULL; }
static inline u64 ht(u64 x) { return (x ^ (x >> 29)) * 0xC2B2AE3D27D4EB4FULL; }
static inline u64 slot_of(u64 x, u32 shift) { return ht(x) >> shift; }
static inline u32 shift_of(u64 cap) { return 64 - (u32)__builtin_ctzll(cap); }
static inline u64 mix64(u64 x) {
  x ^= x >> 31;
  x *= 0x7fb5d329728ea185ULL;
  x ^= x >> 27;
  x *= 0x81dadef4bc2dd44dULL;
  x ^= x >> 33;
  return x;
}

// ---------------------------------------------------------------- timestamps
static inline i64 days_from_civil(i64 y, i64 m, i64 d) {
  y -= m <= 2;
  i64 era = (y >= 0 ? y : y - 399) / 400;
  i64 yoe = y - era * 400;
  i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

// Days since epoch of Jan 1 for years 1900..2299, and cumulative month days.
static i32 YDAYS[400];
static u8 YLEAP[400];
static const u16 CUM[13] = {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
static void init_years() {
  for (int i = 0; i < 400; i++) {
    int y = 1900 + i;
    YDAYS[i] = (i32)days_from_civil(y, 1, 1);
    YLEAP[i] = (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
  }
}
static inline i64 fast_days(int y, int m, int d) {
  unsigned yi = (unsigned)(y - 1900);
  if (UNLIKELY(yi >= 400)) return days_from_civil(y, m, d);
  return YDAYS[yi] + CUM[m] + (m > 2 ? YLEAP[yi] : 0) + d - 1;
}

static inline bool is_digit(u8 c) { return (u8)(c - '0') < 10; }
static inline int d2(const u8* p) { return (p[0] - '0') * 10 + (p[1] - '0'); }

static inline u64 load8(const u8* p) {
  u64 v;
  memcpy(&v, p, 8);
  return v;
}
// True if all 8 bytes (masked by `m`, 0x00 or 0xFF per byte) of x are ASCII digits.
static inline bool all_digits8(u64 x, u64 m) {
  u64 d = (x ^ 0x3030303030303030ULL) & m;              // digit -> 0..9
  return (((d + 0x7676767676767676ULL) | d) & 0x8080808080808080ULL & m) == 0;
}
// 8 ASCII digits (first char in lowest byte) -> value.
static inline u64 swar8(u64 v) {
  v = (v & 0x0F0F0F0F0F0F0F0FULL) * 2561 >> 8;
  v = (v & 0x00FF00FF00FF00FFULL) * 6553601 >> 16;
  return (v & 0x0000FFFF0000FFFFULL) * 42949672960001ULL >> 32;
}

static const u8 DIM[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

// Per-thread cache: raw "YYYY-MM-DD" digits -> days since epoch (or BAD_DAYS).
static const i32 BAD_DAYS = INT32_MIN;
struct DateCache {
  u64 key[1024];
  i32 days[1024];
};
static thread_local DateCache t_dc;

static i32 days_of_key(const u8* p) {
  int year = (p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0');
  int mon = d2(p + 5), day = d2(p + 8);
  if ((unsigned)(mon - 1) > 11 || day < 1) return BAD_DAYS;
  int maxd = DIM[mon];
  if (mon == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) maxd = 29;
  if (day > maxd) return BAD_DAYS;
  return (i32)fast_days(year, mon, day);
}

#if defined(__SSE4_1__)
#include <immintrin.h>
// SSE version: validates all 19 bytes with two 16-byte compares and computes
// seconds-of-day with pshufb + pmaddubsw + pmaddwd. The date goes through the cache.
static inline bool parse_civil(const u8* p, i64& secs) {
  const __m128i x = _mm_loadu_si128((const __m128i*)(p + 3));  // Y-MM-DDTHH:MM:SS
  const __m128i y = _mm_loadu_si128((const __m128i*)p);        // YYYY-...
  const __m128i lo = _mm_set1_epi8('0' - 1), hi = _mm_set1_epi8('9' + 1);
  u32 dx = (u32)_mm_movemask_epi8(_mm_and_si128(_mm_cmpgt_epi8(x, lo), _mm_cmplt_epi8(x, hi)));
  u32 dy = (u32)_mm_movemask_epi8(_mm_and_si128(_mm_cmpgt_epi8(y, lo), _mm_cmplt_epi8(y, hi)));
  const __m128i tmpl = _mm_setr_epi8(0, '-', 0, 0, '-', 0, 0, 'T', 0, 0, ':', 0, 0, ':', 0, 0);
  u32 sx = (u32)_mm_movemask_epi8(_mm_cmpeq_epi8(x, tmpl));
  // digits at x lanes 0,2,3,5,6,8,9,11,12,14,15; separators at 1,4,7,10,13; y lanes 0..2 digits
  const u32 DIG = 0xDB6D, SEP = 0x2492;
  if (UNLIKELY(((dx & DIG) != DIG) | ((sx & SEP) != SEP) | ((dy & 7) != 7))) return false;
  // HH MM SS DD pairs -> 16-bit values
  const __m128i shuf = _mm_setr_epi8(8, 9, 11, 12, 14, 15, 5, 6, -1, -1, -1, -1, -1, -1, -1, -1);
  __m128i d = _mm_sub_epi8(_mm_shuffle_epi8(x, shuf), _mm_setr_epi8('0', '0', '0', '0', '0', '0', '0', '0', 0, 0, 0, 0, 0, 0, 0, 0));
  __m128i v = _mm_maddubs_epi16(d, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0, 0, 0, 0, 0));
  u32 over = (u32)_mm_movemask_epi8(_mm_cmpgt_epi16(v, _mm_setr_epi16(23, 59, 59, 32767, 32767, 32767, 32767, 32767)));
  if (UNLIKELY(over)) return false;
  __m128i t = _mm_madd_epi16(v, _mm_setr_epi16(3600, 60, 1, 0, 0, 0, 0, 0));
  u32 sod = (u32)_mm_cvtsi128_si32(t) + (u32)_mm_extract_epi32(t, 1);
  u64 a = load8(p), b = load8(p + 8);
  const u64 ma = 0x00FFFF00FFFFFFFFULL;
  u64 key = (a & ma) | ((b & 0xFF) << 32) | ((b & 0xFF00) << 48);
  u32 h = (u32)((key * 0x9E3779B97F4A7C15ULL) >> 54);
  i32 days;
  if (LIKELY(t_dc.key[h] == key)) {
    days = t_dc.days[h];
  } else {
    days = days_of_key(p);
    t_dc.key[h] = key;
    t_dc.days[h] = days;
  }
  if (UNLIKELY(days == BAD_DAYS)) return false;
  secs = (i64)days * 86400 + sod;
  return true;
}
#else
// Calendar part "YYYY-MM-DDTHH:MM:SS" (19 bytes) -> seconds. Reads 24 bytes.
static inline bool parse_civil(const u8* p, i64& secs) {
  u64 a = load8(p), b = load8(p + 8), c = load8(p + 16);
  // a: Y Y Y Y - M M -   b: D D T H H : M M   c: : S S ...
  const u64 ma = 0x00FFFF00FFFFFFFFULL, mb = 0xFFFF00FFFF00FFFFULL, mc = 0x0000000000FFFF00ULL;
  const u64 H8 = 0x8080808080808080ULL, Z = 0x3030303030303030ULL, A = 0x7676767676767676ULL;
  u64 da = (a ^ Z) & ma, db = (b ^ Z) & mb, dc = (c ^ Z) & mc;
  u64 err = ((((da + A) | da) & ma) | (((db + A) | db) & mb) | (((dc + A) | dc) & mc)) & H8;
  err |= ((a & ~ma) ^ 0x2D00002D00000000ULL) | ((b & ~mb) ^ 0x00003A0000540000ULL) | ((c & 0xFF) ^ ':');
  if (UNLIKELY(err)) return false;
  u64 key = (a & ma) | ((b & 0xFF) << 32) | ((b & 0xFF00) << 48);
  u32 h = (u32)((key * 0x9E3779B97F4A7C15ULL) >> 54);
  i32 days;
  if (LIKELY(t_dc.key[h] == key)) {
    days = t_dc.days[h];
  } else {
    days = days_of_key(p);
    t_dc.key[h] = key;
    t_dc.days[h] = days;
  }
  if (UNLIKELY(days == BAD_DAYS)) return false;
  u32 hh = (u32)(((b >> 24) & 0xF) * 10 + ((b >> 32) & 0xF));
  u32 mm = (u32)(((b >> 48) & 0xF) * 10 + ((b >> 56) & 0xF));
  u32 ss = (u32)(((c >> 8) & 0xF) * 10 + ((c >> 16) & 0xF));
  if (UNLIKELY((hh > 23) | (mm > 59) | (ss > 59))) return false;
  secs = (i64)days * 86400 + hh * 3600 + mm * 60 + ss;
  return true;
}
#endif

// Parses F1 / F2 / F3 into epoch milliseconds. False for any bad form.
// May read up to 24 bytes from p (the input is padded).
// ingest_ts is always valid F1 (rules), so its digits need no re-validation:
// date through the cache, time of day by SWAR. Anything unexpected falls back.
static inline bool parse_ts(const u8* p, u32 n, i64& out);
static inline bool parse_ingest(const u8* p, u32 n, i64& out) {
  if (UNLIKELY(n != 20 || p[10] != 'T' || p[19] != 'Z')) return parse_ts(p, n, out);
  u64 a = load8(p), b = load8(p + 8), c = load8(p + 16);
  const u64 ma = 0x00FFFF00FFFFFFFFULL;
  u64 key = (a & ma) | ((b & 0xFF) << 32) | ((b & 0xFF00) << 48);
  u32 h = (u32)((key * 0x9E3779B97F4A7C15ULL) >> 54);
  i32 days;
  if (LIKELY(t_dc.key[h] == key)) {
    days = t_dc.days[h];
  } else {
    days = days_of_key(p);
    t_dc.key[h] = key;
    t_dc.days[h] = days;
  }
  if (UNLIKELY(days == BAD_DAYS)) return parse_ts(p, n, out);
  u32 hh = (u32)(((b >> 24) & 0xF) * 10 + ((b >> 32) & 0xF));
  u32 mm = (u32)(((b >> 48) & 0xF) * 10 + ((b >> 56) & 0xF));
  u32 ss = (u32)(((c >> 8) & 0xF) * 10 + ((c >> 16) & 0xF));
  out = ((i64)days * 86400 + hh * 3600 + mm * 60 + ss) * 1000;
  return true;
}

__attribute__((always_inline)) static inline bool parse_ts(const u8* p, u32 n, i64& out) {
  if (n == 20 && p[4] == '-') {
    i64 secs;
    if (p[19] != 'Z' || !parse_civil(p, secs)) return false;
    out = secs * 1000;
    return true;
  }
  if (n == 25 && p[4] == '-') {
    i64 secs;
    if (!parse_civil(p, secs)) return false;
    u8 sign = p[19];
    if (sign != '+' && sign != '-') return false;
    if (!is_digit(p[20]) || !is_digit(p[21]) || p[22] != ':' || !is_digit(p[23]) || !is_digit(p[24])) return false;
    int oh = d2(p + 20), om = d2(p + 23);
    if (oh > 23 || om > 59) return false;
    i64 off = oh * 3600 + om * 60;
    secs += sign == '+' ? -off : off;
    out = secs * 1000;
    return true;
  }
  if (n == 13) {  // the common F3 width
    u64 hi = load8(p), lo = load8(p + 5);
    if (!all_digits8(hi, ~0ULL) || !all_digits8(lo, ~0ULL)) return false;
    out = (i64)(swar8(hi) / 1000 * 100000000ULL + swar8(lo));
    return true;
  }
  if (n == 0) return false;
  // F3: digits only, leading zeros allowed.
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  if (n - i > 19) {
    for (u32 k = i; k < n; k++)
      if (!is_digit(p[k])) return false;
    return false;  // too large: outside any window
  }
  u64 v = 0;
  for (u32 k = i; k < n; k++) {
    u8 c = p[k];
    if (!is_digit(c)) return false;
    v = v * 10 + (c - '0');
  }
  if (v > (u64)INT64_MAX) return false;
  out = (i64)v;
  return true;
}

// ---------------------------------------------------------------- string codes
// Compact: [b0][0-9a-z]{0,8} -> b0<<48 | len<<44 | base36(rest). Others interned.
static std::mutex g_intern_mu;
static std::unordered_map<std::string, u32>* g_intern_map;
static std::vector<std::string>* g_intern_strs;
static const u64 INTERN_BIT = 1ULL << 63;

// b36 digit value 0..35, 0x80 for anything else.
static u8 B36T[256];
static void init_b36() {
  for (int i = 0; i < 256; i++) B36T[i] = 0x80;
  for (int i = 0; i < 10; i++) B36T['0' + i] = (u8)i;
  for (int i = 0; i < 26; i++) B36T['a' + i] = (u8)(10 + i);
}

// Long ids of the v5 shape: one prefix byte + 32 lowercase hex digits. A
// lock-free table maps (prefix, 128-bit value) to a stable slot; the code is
// INTERN_BIT | HEX_BIT | slot, and the text is rebuilt from the slot.
static const u64 HEX_BIT = 1ULL << 62;
struct HSlot {
  std::atomic<u64> state;  // 0 empty, 1 being written, 0x100 | prefix when ready
  u64 hi, lo;
};
// Table 0: user ids (events, clicks, links). Table 1: click ids. Long event ids
// need no table: they dedup on their full 128-bit value (see LRec).
static const u64 TAB_BIT = 1ULL << 61;
struct HexTab {
  HSlot* s = nullptr;
  u64 mask = 0;
  std::atomic<u64> count{0};
  u64 limit = 0;
};
static HexTab g_ht[2];
static void hex_alloc(int k, size_t want) {
  HexTab& T = g_ht[k];
  size_t cap = 1 << 12;
  while (cap < want) cap <<= 1;
  T.s = (HSlot*)mmap(nullptr, cap * sizeof(HSlot), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                     -1, 0);
  if ((void*)T.s == MAP_FAILED) {
    T.s = nullptr;
    return;
  }
  T.mask = cap - 1;
  T.limit = cap / 4 * 3;
}
static inline bool hex32(const u8* p, u64& hi, u64& lo) {
#if defined(__SSSE3__)
  const __m128i a = _mm_loadu_si128((const __m128i*)p), b = _mm_loadu_si128((const __m128i*)(p + 16));
  const __m128i c0 = _mm_set1_epi8('0' - 1), c9 = _mm_set1_epi8('9' + 1), ca = _mm_set1_epi8('a' - 1),
                cf = _mm_set1_epi8('f' + 1);
  __m128i da = _mm_and_si128(_mm_cmpgt_epi8(a, c0), _mm_cmplt_epi8(a, c9));
  __m128i db = _mm_and_si128(_mm_cmpgt_epi8(b, c0), _mm_cmplt_epi8(b, c9));
  __m128i la = _mm_and_si128(_mm_cmpgt_epi8(a, ca), _mm_cmplt_epi8(a, cf));
  __m128i lb = _mm_and_si128(_mm_cmpgt_epi8(b, ca), _mm_cmplt_epi8(b, cf));
  if (_mm_movemask_epi8(_mm_and_si128(_mm_or_si128(da, la), _mm_or_si128(db, lb))) != 0xFFFF) return false;
  // nibble = c - '0' - (letter ? 39 : 0)
  __m128i na = _mm_sub_epi8(_mm_sub_epi8(a, _mm_set1_epi8('0')), _mm_and_si128(la, _mm_set1_epi8(39)));
  __m128i nb = _mm_sub_epi8(_mm_sub_epi8(b, _mm_set1_epi8('0')), _mm_and_si128(lb, _mm_set1_epi8(39)));
  const __m128i w = _mm_set1_epi16(0x0110);  // bytes (16, 1): first char is the high nibble
  __m128i pa = _mm_maddubs_epi16(na, w), pb = _mm_maddubs_epi16(nb, w);
  __m128i packed = _mm_packus_epi16(pa, pb);
  hi = (u64)_mm_cvtsi128_si64(packed);
  lo = (u64)_mm_cvtsi128_si64(_mm_unpackhi_epi64(packed, packed));
  return true;
#else
  u8 out[16];
  for (int i = 0; i < 16; i++) {
    int v[2];
    for (int j = 0; j < 2; j++) {
      u8 c = p[2 * i + j];
      if (c >= '0' && c <= '9')
        v[j] = c - '0';
      else if (c >= 'a' && c <= 'f')
        v[j] = c - 'a' + 10;
      else
        return false;
    }
    out[i] = (u8)(v[0] * 16 + v[1]);
  }
  memcpy(&hi, out, 8);
  memcpy(&lo, out + 8, 8);
  return true;
#endif
}
static u64 intern_slow(const u8* p, u32 n);
static u64 hex_code(int k, const u8* p, u64 hi, u64 lo) {
  HexTab& T = g_ht[k];
  const u64 ready = 0x100 | p[0];
  const u64 tag = INTERN_BIT | HEX_BIT | ((u64)k * TAB_BIT);
  u64 h = ((hi * 0x9E3779B97F4A7C15ULL) ^ (lo * 0xC2B2AE3D27D4EB4FULL) ^ (ready << 7));
  h = (h ^ (h >> 29)) & T.mask;
  for (;;) {
    HSlot& sl = T.s[h];
    u64 st = sl.state.load(std::memory_order_acquire);
    if (st == 0) {
      if (T.count.load(std::memory_order_relaxed) >= T.limit) return intern_slow(p, 33);  // table full
      u64 z = 0;
      if (sl.state.compare_exchange_strong(z, 1, std::memory_order_acquire)) {
        sl.hi = hi;
        sl.lo = lo;
        sl.state.store(ready, std::memory_order_release);
        T.count.fetch_add(1, std::memory_order_relaxed);
        return tag | h;
      }
      continue;  // lost the race: re-read this slot
    }
    while (st == 1) st = sl.state.load(std::memory_order_acquire);
    if (st == ready && sl.hi == hi && sl.lo == lo) return tag | h;
    h = (h + 1) & T.mask;
  }
}
static inline u64 long_code(const u8* p, u32 n, int k) {
  u64 hi, lo;
  if (n == 33 && g_ht[k].s && hex32(p + 1, hi, lo)) return hex_code(k, p, hi, lo);
  return intern_slow(p, n);
}

static u64 intern_slow(const u8* p, u32 n) {
  std::string s((const char*)p, n);
  std::lock_guard<std::mutex> g(g_intern_mu);
  auto it = g_intern_map->find(s);
  if (it != g_intern_map->end()) return INTERN_BIT | it->second;
  u32 id = (u32)g_intern_strs->size();
  g_intern_strs->push_back(s);
  (*g_intern_map)[s] = id;
  return INTERN_BIT | id;
}

#if defined(__BMI2__) && defined(__SSE2__)
#include <immintrin.h>
// Compact code: b0 << 52 | len << 48 | digits (6 bits each, first char highest).
__attribute__((always_inline)) static inline u64 code_of(const u8* p, u32 n, int k = 0) {
  if (LIKELY(n - 1 < 9)) {
    const __m128i iota = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    __m128i lanes = _mm_cmplt_epi8(iota, _mm_set1_epi8((char)(n - 1)));
    __m128i v = _mm_and_si128(_mm_loadl_epi64((const __m128i*)(p + 1)), lanes);
    __m128i isd = _mm_and_si128(_mm_cmpgt_epi8(v, _mm_set1_epi8('0' - 1)), _mm_cmplt_epi8(v, _mm_set1_epi8('9' + 1)));
    __m128i isl = _mm_and_si128(_mm_cmpgt_epi8(v, _mm_set1_epi8('a' - 1)), _mm_cmplt_epi8(v, _mm_set1_epi8('z' + 1)));
    __m128i ok = _mm_or_si128(isd, isl);
    if (LIKELY(_mm_movemask_epi8(_mm_andnot_si128(ok, lanes)) == 0)) {
      __m128i val = _mm_sub_epi8(_mm_sub_epi8(v, _mm_set1_epi8('0')), _mm_and_si128(isl, _mm_set1_epi8(39)));
      val = _mm_and_si128(val, lanes);
      u64 x = __builtin_bswap64((u64)_mm_cvtsi128_si64(val));
      return ((u64)p[0] << 52) | ((u64)n << 48) | _pext_u64(x, 0x3F3F3F3F3F3F3F3FULL);
    }
  }
  return long_code(p, n, k);
}
#define SS_CODE_OF_DEFINED
#endif
// Compact code: b0 << 52 | len << 48 | digits (6 bits each, first char highest).
#ifndef SS_CODE_OF_DEFINED
__attribute__((always_inline)) static inline u64 code_of(const u8* p, u32 n, int k = 0) {
  if (LIKELY(n - 1 < 9)) {
    u64 v = 0;
    u8 bad = 0;
    switch (n) {
#define D(k)                    \
  {                             \
    u8 d = B36T[p[k]];          \
    bad |= d;                   \
    v |= (u64)d << (6 * (8 - k)); \
  }
      case 9: D(8)
      case 8: D(7)
      case 7: D(6)
      case 6: D(5)
      case 5: D(4)
      case 4: D(3)
      case 3: D(2)
      case 2: D(1)
      default: break;
#undef D
    }
    if (LIKELY(!(bad & 0x80))) return ((u64)p[0] << 52) | ((u64)n << 48) | v;
  }
  return long_code(p, n, k);
}
#endif

// Decodes into buf (>= 16 bytes for compact); returns pointer and length.
static inline const char* decode(u64 c, char* buf, u32& len) {
  if ((c & (INTERN_BIT | HEX_BIT)) == (INTERN_BIT | HEX_BIT)) {
    const HSlot& sl = g_ht[(c & TAB_BIT) ? 1 : 0].s[c & (TAB_BIT - 1)];
    buf[0] = (char)(sl.state.load(std::memory_order_relaxed) & 0xff);
    u8 bytes[16];
    memcpy(bytes, &sl.hi, 8);
    memcpy(bytes + 8, &sl.lo, 8);
    for (int i = 0; i < 16; i++) {
      buf[1 + 2 * i] = "0123456789abcdef"[bytes[i] >> 4];
      buf[2 + 2 * i] = "0123456789abcdef"[bytes[i] & 15];
    }
    len = 33;
    return buf;
  }
  if (c & INTERN_BIT) {
    const std::string& s = (*g_intern_strs)[(u32)(c & 0xffffffffu)];  // HEX_BIT clear here
    len = (u32)s.size();
    return s.data();
  }
  u32 n = (u32)((c >> 48) & 15);
  buf[0] = (char)((c >> 52) & 0xff);
  for (u32 k = 1; k < n; k++) buf[k] = "0123456789abcdefghijklmnopqrstuvwxyz"[(c >> (6 * (8 - k))) & 63];
  len = n;
  return buf;
}

static int code_cmp(u64 a, u64 b) {
  if (a == b) return 0;
  if (!((a | b) & INTERN_BIT) && (a >> 48) == (b >> 48)) return a < b ? -1 : 1;
  char ba[40], bb[40];
  u32 la, lb;
  const char* sa = decode(a, ba, la);
  const char* sb = decode(b, bb, lb);
  int r = memcmp(sa, sb, la < lb ? la : lb);
  if (r) return r;
  return la < lb ? -1 : (la > lb ? 1 : 0);
}

// ---------------------------------------------------------------- CSV
struct Field {
  const u8* p;
  u32 n;
};

// Splits [s,e) into exactly nf fields. False if malformed.
static inline bool split(const u8* s, const u8* e, Field* f, int nf) {
  int k = 0;
  const u8* fs = s;
  bool q = false;
  for (const u8* p = s;; ++p) {
    if (p == e || *p == ',') {
      if (k >= nf) return false;
      u32 len = (u32)(p - fs);
      if (UNLIKELY(q)) {
        if (len < 2 || fs[0] != '"' || fs[len - 1] != '"') return false;
        for (u32 i = 1; i + 1 < len; i++)
          if (fs[i] == '"') return false;
        f[k].p = fs + 1;
        f[k].n = len - 2;
      } else {
        f[k].p = fs;
        f[k].n = len;
      }
      k++;
      if (p == e) break;
      fs = p + 1;
      q = false;
    } else if (*p == '"') {
      q = true;
    }
  }
  return k == nf;
}

// ---------------------------------------------------------------- SIMD scan
#if defined(__AVX512BW__) && defined(SS_AVX512)
#include <immintrin.h>
static inline void masks64(const u8* p, u64& sep, u64& nl, u64& qt) {
  __m512i v = _mm512_loadu_si512((const void*)p);
  u64 c = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(','));
  nl = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8('\n'));
  qt = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8('"'));
  sep = c | nl;
}
#elif defined(__AVX2__)
#include <immintrin.h>
static inline void masks64(const u8* p, u64& sep, u64& nl, u64& qt) {
  __m256i a = _mm256_loadu_si256((const __m256i*)p), b = _mm256_loadu_si256((const __m256i*)(p + 32));
  __m256i C = _mm256_set1_epi8(','), N = _mm256_set1_epi8('\n'), Q = _mm256_set1_epi8('"');
  u64 c = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, C)) |
          ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, C)) << 32);
  nl = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, N)) |
       ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, N)) << 32);
  qt = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, Q)) |
       ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, Q)) << 32);
  sep = c | nl;
}
#elif defined(__ARM_NEON)
#include <arm_neon.h>
static inline u64 neon_mask(uint8x16_t a, uint8x16_t b, uint8x16_t c, uint8x16_t d) {
  const uint8x16_t bit = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
  uint8x16_t s0 = vpaddq_u8(vandq_u8(a, bit), vandq_u8(b, bit));
  uint8x16_t s1 = vpaddq_u8(vandq_u8(c, bit), vandq_u8(d, bit));
  s0 = vpaddq_u8(s0, s1);
  s0 = vpaddq_u8(s0, s0);
  return vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0);
}
static inline void masks64(const u8* p, u64& sep, u64& nl, u64& qt) {
  uint8x16_t a = vld1q_u8(p), b = vld1q_u8(p + 16), c = vld1q_u8(p + 32), d = vld1q_u8(p + 48);
  uint8x16_t C = vdupq_n_u8(','), N = vdupq_n_u8('\n'), Q = vdupq_n_u8('"');
  nl = neon_mask(vceqq_u8(a, N), vceqq_u8(b, N), vceqq_u8(c, N), vceqq_u8(d, N));
  sep = neon_mask(vorrq_u8(vceqq_u8(a, C), vceqq_u8(a, N)), vorrq_u8(vceqq_u8(b, C), vceqq_u8(b, N)),
                  vorrq_u8(vceqq_u8(c, C), vceqq_u8(c, N)), vorrq_u8(vceqq_u8(d, C), vceqq_u8(d, N)));
  qt = neon_mask(vceqq_u8(a, Q), vceqq_u8(b, Q), vceqq_u8(c, Q), vceqq_u8(d, Q));
}
#else
static inline void masks64(const u8* p, u64& sep, u64& nl, u64& qt) {
  sep = nl = qt = 0;
  for (int i = 0; i < 64; i++) {
    u64 bit = 1ULL << i;
    if (p[i] == ',') sep |= bit;
    if (p[i] == '\n') nl |= bit, sep |= bit;
    if (p[i] == '"') qt |= bit;
  }
}
#endif

// Resolves NF fields of one line from comma offsets; strips whole-field quotes.
// q = number of quote characters in the line. False if malformed.
template <int NF>
__attribute__((always_inline)) static inline bool fields_of(const u8* ls, const u32* ends, u32 q, Field* f) {
  u32 st = 0;
  if (q == 0) {
    for (int i = 0; i < NF; i++) {
      f[i].p = ls + st;
      f[i].n = ends[i] - st;
      st = ends[i] + 1;
    }
    return true;
  }
  u32 quoted = 0;
  bool bad = false;
  for (int i = 0; i < NF; i++) {
    u32 en = ends[i];
    u32 len = en - st;
    u32 isq = (len != 0) & (ls[st] == '"');
    bad |= isq & ((len < 2) | (ls[en - (len != 0)] != '"'));
    quoted += isq;
    f[i].p = ls + st + isq;
    f[i].n = len - 2 * isq;
    st = en + 1;
  }
  return !bad && q == 2 * quoted;
}

// Calls cb(fields) for every well-formed line in [s, e). Reads up to 64 bytes past e.
template <int NF, class CB>
static inline void scan_lines_old(const u8* s, const u8* e, CB&& cb) {
  const u8* ls = s;
  u32 ends[NF];
  u32 k = 0, q = 0;
  size_t len = (size_t)(e - s);
  Field f[NF];
  for (size_t off = 0; off < len; off += 64) {
    u64 sep, nl, qt;
    masks64(s + off, sep, nl, qt);
    size_t rem = len - off;
    if (rem < 64) {
      u64 lim = (1ULL << rem) - 1;
      sep &= lim;
      nl &= lim;
      qt &= lim;
    }
    while (sep) {
      int b = __builtin_ctzll(sep);
      const u8* p = s + off + b;
      if ((nl >> b) & 1) {
        u64 low = (2ULL << b) - 1;
        q += (u32)__builtin_popcountll(qt & low);
        qt &= ~low;
        if (k == NF - 1) {
          ends[k] = (u32)(p - ls);
          if (fields_of<NF>(ls, ends, q, f)) cb(f);
        }
        ls = p + 1;
        k = 0;
        q = 0;
      } else {
        if (k < NF - 1) ends[k] = (u32)(p - ls);
        k++;
      }
      sep &= sep - 1;
    }
    q += (u32)__builtin_popcountll(qt);
  }
  if (ls < e && k == NF - 1) {  // last line without '\n'
    ends[k] = (u32)(e - ls);
    if (fields_of<NF>(ls, ends, q, f)) cb(f);
  }
}
// Quote characters among window offsets [r0, r1), from per-block quote masks.
static inline u32 qcount(const u64* qm, u32 r0, u32 r1) {
  u32 b0 = r0 >> 6, b1 = r1 >> 6;
  u64 m0 = qm[b0] & (~0ULL << (r0 & 63));
  u64 tail = (1ULL << (r1 & 63)) - 1;
  if (b0 == b1) return (u32)__builtin_popcountll(m0 & tail);
  u32 c = (u32)__builtin_popcountll(m0);
  for (u32 b = b0 + 1; b < b1; b++) c += (u32)__builtin_popcountll(qm[b]);
  return c + (u32)__builtin_popcountll(qm[b1] & tail);
}

static bool g_oldscan = getenv("SS_OLDSCAN") != nullptr;

// Fields of one line from separator offsets (relative to the line start ls),
// stripping quotes by visiting only the quote characters: they must come in
// pairs that sit exactly on the first and last byte of one field.
// qm/r0/le locate the line's quote bits in window coordinates.
template <int NF>
static inline bool fields_q(const u8* ls, const u32* ends, const u64* qm, u32 r0, u32 le, Field* f) {
  u32 st = 0;
  for (int i = 0; i < NF; i++) {
    f[i].p = ls + st;
    f[i].n = ends[i] - st;
    st = ends[i] + 1;
  }
  u32 b0 = r0 >> 6, b1 = le >> 6;
  u64 m = qm[b0] & (~0ULL << (r0 & 63));
  if (b0 == b1) m &= (1ULL << (le & 63)) - 1;
  if (LIKELY(m == 0 && b0 == b1)) return true;
  u32 open = 0, x1 = 0;
  for (u32 b = b0;; ) {
    while (m) {
      u32 x = b * 64 + (u32)__builtin_ctzll(m) - r0;
      m &= m - 1;
      if (!open) {
        x1 = x;
        open = 1;
      } else {
        u32 k = 0;
        for (int i = 0; i < NF - 1; i++) k += (ends[i] < x1);
        u32 start = k ? ends[k - 1] + 1 : 0;
        if (x1 != start || x != ends[k] - 1) return false;
        f[k].p++;
        f[k].n -= 2;
        open = 0;
      }
    }
    if (++b > b1) break;
    m = qm[b];
    if (b == b1) m &= (1ULL << (le & 63)) - 1;
  }
  return !open;
}

// Calls cb(fields) for every well-formed line in [s, e). Reads up to 64 bytes past e.
// Two stages per window: (1) branch-free SIMD flattening of separator positions
// into an index plus per-block quote masks; (2) a walk over lines that checks
// the NF-th separator is the newline and counts quotes by popcount.
template <int NF, class CB>
static inline void scan_lines_idx(const u8* s, const u8* e, CB&& cb) {
  static thread_local std::vector<u32> IDX;
  static thread_local std::vector<u64> QM;
  static thread_local std::vector<u32> NLI;
  Field f[NF];
  u32 ends[NF];
  const u8* a = s;  // window start, always a line start
  size_t W = 32768;
  while (a < e) {
    size_t wl = std::min<size_t>(W, (size_t)(e - a));
    bool last = a + wl == e;
    size_t nblk = (wl + 63) / 64;
    if (IDX.size() < nblk * 64 + 16) IDX.resize(nblk * 64 + 16);
    if (QM.size() < nblk + 2) QM.resize(nblk + 2);
    if (NLI.size() < nblk * 64 + 16) NLI.resize(nblk * 64 + 16);
    u32* idx = IDX.data();
    u64* qm = QM.data();
    u32* nli = NLI.data();
    u32 n = 0, nn = 0;
    for (size_t bi = 0; bi < nblk; bi++) {
      u64 sep, nl, qt;
      masks64(a + bi * 64, sep, nl, qt);
      size_t rem = wl - bi * 64;
      if (rem < 64) {
        u64 lim = (1ULL << rem) - 1;
        sep &= lim;
        qt &= lim;
        nl &= lim;
      }
      qm[bi] = qt;
      {
        const u64 H = 1ULL << 63;
        u32 nc = (u32)__builtin_popcountll(nl);
        u32* q = nli + nn;
        u32 base = (u32)(bi * 64);
        q[0] = base + (u32)__builtin_ctzll(nl | H);
        nl &= nl - 1;
        q[1] = base + (u32)__builtin_ctzll(nl | H);
        nl &= nl - 1;
        for (u32 w = 2; w < nc; w++) {
          q[w] = base + (u32)__builtin_ctzll(nl);
          nl &= nl - 1;
        }
        nn += nc;
      }
      u32 cnt = (u32)__builtin_popcountll(sep);
      u32 base = (u32)(bi * 64);
      u32* o = idx + n;
      const u64 H = 1ULL << 63;
      for (u32 w = 0; w < cnt; w += 8) {
        o[w + 0] = base + (u32)__builtin_ctzll(sep | H);
        sep &= sep - 1;
        o[w + 1] = base + (u32)__builtin_ctzll(sep | H);
        sep &= sep - 1;
        o[w + 2] = base + (u32)__builtin_ctzll(sep | H);
        sep &= sep - 1;
        o[w + 3] = base + (u32)__builtin_ctzll(sep | H);
        sep &= sep - 1;
        o[w + 4] = base + (u32)__builtin_ctzll(sep | H);
        sep &= sep - 1;
        o[w + 5] = base + (u32)__builtin_ctzll(sep | H);
        sep &= sep - 1;
        o[w + 6] = base + (u32)__builtin_ctzll(sep | H);
        sep &= sep - 1;
        o[w + 7] = base + (u32)__builtin_ctzll(sep | H);
        sep &= sep - 1;
      }
      n += cnt;
    }
    qm[nblk] = 0;
    u32 j = 0, r0 = 0, li = 0;  // r0 = current line start (window offset)
    bool progressed = false;
    // A line is well-shaped iff its NF-th separator is the next newline.
    while (li < nn) {
      u32 le = nli[li++];
      progressed = true;
      if (LIKELY(j + NF - 1 < n && idx[j + NF - 1] == le)) {
        for (int k = 0; k < NF; k++) ends[k] = idx[j + k] - r0;
        if (fields_q<NF>(a + r0, ends, qm, r0, le, f)) cb(f);
        j += NF;
      } else {  // wrong field count: dropped
        while (idx[j] != le) j++;
        j++;
      }
      r0 = le + 1;
    }
    if (last) {
      // What remains is a final line without '\n' (only commas indexed).
      if (r0 < wl && n - j == NF - 1) {
        for (int k = 0; k < NF - 1; k++) ends[k] = idx[j + k] - r0;
        ends[NF - 1] = (u32)wl - r0;
        if (fields_q<NF>(a + r0, ends, qm, r0, (u32)wl, f)) cb(f);
      }
      break;
    }
    if (progressed) {
      a += r0;
      W = 32768;
    } else {
      W *= 2;  // a line longer than the window
    }
  }
}

// k-th set bit (0-based) of m, k < popcount(m).
static inline u32 nth_bit(u64 m, u32 k) {
#if defined(__BMI2__)
  return (u32)__builtin_ctzll(_pdep_u64(1ULL << k, m) | (1ULL << 63));  // 63 if absent (unused)
#else
  for (u32 i = 0; i < k; i++) m &= m - 1;
  return (u32)__builtin_ctzll(m | (1ULL << 63));
#endif
}
static int g_scanmode = getenv("SS_OLDSCAN") ? 2 : (getenv("SS_IDXSCAN") ? 1 : 0);

// Line-at-a-time scanner: a 128-byte window at the line start gives the
// newline (tzcnt), the commas before it (exactly NF-1 or the line is dropped),
// each comma's position (pdep + tzcnt, no loops) and the quote count. Lines of
// 128 bytes or more, and the last few lines of the range, take the exact index
// scanner. Calls cb(fields) for every well-formed line in [s, e).
template <int NF, class CB>
static inline void scan_lines(const u8* s, const u8* e, CB&& cb) {
  if (UNLIKELY(g_scanmode == 2)) return scan_lines_old<NF>(s, e, cb);
  if (UNLIKELY(g_scanmode == 1)) return scan_lines_idx<NF>(s, e, cb);
  const u8* p = s;
  Field f[NF];
  u32 ends[NF];
  while (p + 192 <= e) {
    u64 s0, n0, q0;
    masks64(p, s0, n0, q0);
    u64 C0, C1, Q0, Q1;
    u32 L;
    if (n0) {
      L = (u32)__builtin_ctzll(n0);
      u64 below = (1ULL << L) - 1;
      C0 = s0 & below;
      Q0 = q0 & below;
      C1 = 0;
      Q1 = 0;
    } else {
      u64 s1, n1, q1;
      masks64(p + 64, s1, n1, q1);
      if (UNLIKELY(!n1)) {  // 128 bytes or more: exact scanner for this one line
        const u8* nl = (const u8*)memchr(p + 128, '\n', (size_t)(e - (p + 128)));
        if (!nl) break;
        scan_lines_idx<NF>(p, nl + 1, cb);
        p = nl + 1;
        continue;
      }
      L = 64 + (u32)__builtin_ctzll(n1);
      u64 below = (1ULL << (L - 64)) - 1;
      C0 = s0;
      Q0 = q0;
      C1 = s1 & below;
      Q1 = q1 & below;
    }
    u32 pc0 = (u32)__builtin_popcountll(C0);
    if (LIKELY(pc0 + (u32)__builtin_popcountll(C1) == NF - 1)) {
      for (u32 k = 0; k < NF - 1; k++) {
        u32 a = nth_bit(C0, k < pc0 ? k : 0);
        u32 b = 64 + nth_bit(C1, k < pc0 ? 0 : k - pc0);
        ends[k] = k < pc0 ? a : b;
      }
      ends[NF - 1] = L;
      u32 q = (u32)__builtin_popcountll(Q0) + (u32)__builtin_popcountll(Q1);
      if (fields_of<NF>(p, ends, q, f)) cb(f);
    }
    p += L + 1;
  }
  if (p < e) scan_lines_idx<NF>(p, e, cb);
}

static inline bool feq(const Field& f, const char* s) {
  size_t n = strlen(s);
  return f.n == n && memcmp(f.p, s, n) == 0;
}

// ---------------------------------------------------------------- files
struct MFile {
  const u8* p;
  size_t n;
};

static MFile map_file(const std::string& path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) die(("open " + path).c_str());
  struct stat st;
  fstat(fd, &st);
  MFile m{nullptr, (size_t)st.st_size};
  if (m.n) {
    // Reserve one extra page of zeros after the file so SIMD loads may overrun.
    size_t pg = (size_t)sysconf(_SC_PAGESIZE);
    size_t span = (m.n + 128 + pg - 1) / pg * pg;
    void* r = mmap(nullptr, span, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (r == MAP_FAILED) die("mmap reserve");
    void* a = mmap(r, m.n, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0);
    if (a == MAP_FAILED) die("mmap");
    m.p = (const u8*)a;
  }
  close(fd);
  return m;
}

static std::vector<std::string> list_csv(const std::string& dir) {
  std::vector<std::string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (dirent* e = readdir(d)) {
    std::string n = e->d_name;
    if (n.size() >= 4 && n.compare(n.size() - 4, 4, ".csv") == 0) out.push_back(dir + "/" + n);
  }
  closedir(d);
  std::sort(out.begin(), out.end());
  return out;
}

static std::string read_small(const std::string& path) {
  MFile m = map_file(path);
  return std::string((const char*)m.p, m.n);
}

static bool g_prefault = getenv("SS_NO_PREFAULT") == nullptr;
struct Chunk {
  const u8* s;
  const u8* e;
  int kind;  // 0 events, 1 clicks, 2 links
  const u8* fs;
  const u8* fe;  // whole file (mmap mode)
  int fd;        // read mode: file and byte range [off, end) of a file of fsize bytes
  u64 off, end, fsize;
};
static bool g_use_mmap = getenv("SS_MMAP") != nullptr;

// Cuts each file into fixed byte ranges. A chunk owns the lines that start
// inside it; boundaries are resolved by the worker (see resolve_chunk).
struct FileSpan {
  const u8* p;
  size_t n;
};
static void make_chunks(const std::vector<std::string>& files, int kind, size_t target, std::vector<Chunk>& out,
                        size_t& total) {
  for (auto& f : files) {
    if (g_use_mmap) {
      MFile m = map_file(f);
      if (!m.n) continue;
      total += m.n;
      for (size_t s = 0; s < m.n; s += target) {
        size_t e = s + target < m.n ? s + target : m.n;
        out.push_back({m.p + s, m.p + e, kind, m.p, m.p + m.n, -1, 0, 0, 0});
      }
    } else {
      int fd = open(f.c_str(), O_RDONLY);
      if (fd < 0) die(("open " + f).c_str());
      struct stat st;
      fstat(fd, &st);
      u64 n = (u64)st.st_size;
      if (!n) {
        close(fd);
        continue;
      }
      total += n;
      for (u64 s = 0; s < n; s += target)
        out.push_back({nullptr, nullptr, kind, nullptr, nullptr, fd, s, s + target < n ? s + target : n, n});
    }
  }
}

// Line-aligns a raw chunk: starts after the first '\n' at or after s-1 (or
// after the header line for the file's first chunk), ends likewise for e.
#if defined(__linux__) && !defined(MADV_POPULATE_READ)
#define MADV_POPULATE_READ 22
#endif
// Pre-faults a chunk's pages in one call (Linux 5.14+); a no-op elsewhere.
static inline void prefault(const u8* s, const u8* e) {
#if defined(__linux__)
  static const uintptr_t PG = 4096;
  uintptr_t a = (uintptr_t)s & ~(PG - 1), b = ((uintptr_t)e + PG - 1) & ~(PG - 1);
  if (b > a) madvise((void*)a, b - a, MADV_POPULATE_READ);
#else
  (void)s;
  (void)e;
#endif
}

// Read mode: per-thread buffer holding file bytes [boff, boff + bn), zero-padded.
struct ReadBuf {
  u8* p = nullptr;
  size_t cap = 0, n = 0;
  u64 boff = 0;
  int fd = -1;
  void reserve(size_t want) {
    if (want + 128 <= cap) return;
    size_t nc = (want + 128) * 2;
    u8* np = (u8*)aligned_alloc(64, (nc + 63) / 64 * 64);
    if (p) {
      memcpy(np, p, n);
      free(p);
    }
    p = np;
    cap = nc;
  }
  // Ensures bytes [boff, upto) are present (upto <= file size).
  void ensure(u64 upto) {
    if (boff + n >= upto) return;
    reserve((size_t)(upto - boff));
    while (boff + n < upto) {
      ssize_t r = pread(fd, p + n, (size_t)(upto - boff - n), (off_t)(boff + n));
      if (r <= 0) die("pread");
      n += (size_t)r;
    }
    memset(p + n, 0, 128);
  }
};
static thread_local ReadBuf t_rb;

// Line-aligns a read-mode chunk into t_rb; s/e point into the buffer.
static inline bool read_chunk(const Chunk& c, const u8*& s, const u8*& e) {
  ReadBuf& rb = t_rb;
  rb.fd = c.fd;
  rb.boff = c.off == 0 ? 0 : c.off - 1;
  rb.n = 0;
  const u64 fsz = c.fsize;
  u64 want = c.end + 4096 < fsz ? c.end + 4096 : fsz;
  rb.ensure(want);
  // First line start: after the first '\n' at or after boff.
  u64 scanned = 0;
  const u8* x;
  for (;;) {
    x = (const u8*)memchr(rb.p + scanned, '\n', rb.n - scanned);
    if (x) break;
    if (rb.boff + rb.n >= fsz) return false;
    scanned = rb.n;
    rb.ensure(std::min<u64>(fsz, rb.boff + rb.n * 2 + 4096));
  }
  u64 first = rb.boff + (u64)(x - rb.p) + 1;  // file offset of first line start
  if (first >= c.end) return false;
  u64 last_end;
  if (c.end == fsz) {
    rb.ensure(fsz);
    last_end = fsz;
  } else {
    u64 from = c.end - 1;  // the line containing byte end-1 is the last one owned
    for (;;) {
      u64 have = rb.boff + rb.n;
      const u8* y = (const u8*)memchr(rb.p + (from - rb.boff), '\n', (size_t)(have - from));
      if (y) {
        last_end = rb.boff + (u64)(y - rb.p) + 1;
        break;
      }
      if (have >= fsz) {
        last_end = fsz;
        break;
      }
      from = have;
      rb.ensure(std::min<u64>(fsz, rb.boff + rb.n * 2 + 4096));
    }
  }
  s = rb.p + (first - rb.boff);
  e = rb.p + (last_end - rb.boff);
  return true;
}

static inline bool resolve_chunk(const Chunk& c, const u8*& s, const u8*& e) {
  if (!g_use_mmap) return read_chunk(c, s, e);
  const u8* fe = c.fe;
  if (g_prefault) prefault(c.s, c.e);
  const u8* a = c.s == c.fs ? c.fs : c.s - 1;
  const u8* x = (const u8*)memchr(a, '\n', fe - a);
  if (!x || x + 1 >= c.e) return false;
  s = x + 1;
  if (c.e == fe) {
    e = fe;
  } else {
    const u8* y = (const u8*)memchr(c.e - 1, '\n', fe - (c.e - 1));
    e = y ? y + 1 : fe;
  }
  return true;
}

// ---------------------------------------------------------------- globals
static i64 g_ws, g_we;  // window, ms
static i64 g_cs;        // click window start = ws - 7d
static i64 g_ingest_base_s;
static int g_ndays;
static i64 g_wsday;  // floor(ws / DAY)

static std::vector<u16> g_prod_by_num;  // canonical p<n> -> cat (0xFFFF = none)
static std::unordered_map<std::string, u16> g_prod_other;
static u16 g_unknown_cat;
static std::vector<std::string> g_cat_names;  // sorted, id = rank

static std::vector<u32> g_camp_by_num;  // canonical c_<n> -> idx (0xFFFFFFFF none)
static std::unordered_map<std::string, u32> g_camp_other;
static std::vector<std::string> g_camp_ids, g_camp_channels;
static u32 g_ncamp;  // organic = g_ncamp

static inline bool canon_num(const u8* p, u32 n, u32& v) {
  if (n == 0 || n > 9 || p[0] == '0') return false;
  u32 x = 0;
  for (u32 i = 0; i < n; i++) {
    if (!is_digit(p[i])) return false;
    x = x * 10 + (p[i] - '0');
  }
  v = x;
  return true;
}

static inline u16 product_cat(const Field& f) {
  u32 v;
  if (f.n >= 2 && f.p[0] == 'p' && canon_num(f.p + 1, f.n - 1, v)) {
    if (v < g_prod_by_num.size() && g_prod_by_num[v] != 0xFFFF) return g_prod_by_num[v];
    return g_unknown_cat;
  }
  if (g_prod_other.empty()) return g_unknown_cat;
  auto it = g_prod_other.find(std::string((const char*)f.p, f.n));
  return it == g_prod_other.end() ? g_unknown_cat : it->second;
}

static inline u32 campaign_idx(const Field& f) {
  u32 v;
  if (f.n >= 3 && f.p[0] == 'c' && f.p[1] == '_' && canon_num(f.p + 2, f.n - 2, v)) {
    if (v < g_camp_by_num.size()) return g_camp_by_num[v];
    return 0xFFFFFFFFu;
  }
  if (g_camp_other.empty()) return 0xFFFFFFFFu;
  auto it = g_camp_other.find(std::string((const char*)f.p, f.n));
  return it == g_camp_other.end() ? 0xFFFFFFFFu : it->second;
}

static void load_lookups(const std::string& dir) {
  // products
  std::string s = read_small(dir + "/products.csv");
  std::vector<std::pair<std::string, std::string>> prods;
  std::unordered_map<std::string, int> catset;
  {
    size_t pos = s.find('\n');
    pos = pos == std::string::npos ? s.size() : pos + 1;
    while (pos < s.size()) {
      size_t e = s.find('\n', pos);
      if (e == std::string::npos) e = s.size();
      Field f[2];
      if (split((const u8*)s.data() + pos, (const u8*)s.data() + e, f, 2)) {
        prods.push_back({std::string((const char*)f[0].p, f[0].n), std::string((const char*)f[1].p, f[1].n)});
        catset[prods.back().second] = 0;
      }
      pos = e + 1;
    }
  }
  catset["unknown"] = 0;
  for (auto& kv : catset) g_cat_names.push_back(kv.first);
  std::sort(g_cat_names.begin(), g_cat_names.end());
  if (g_cat_names.size() > 256) die("too many categories");
  for (size_t i = 0; i < g_cat_names.size(); i++) catset[g_cat_names[i]] = (int)i;
  g_unknown_cat = (u16)catset["unknown"];
  for (auto& pr : prods) {
    u16 c = (u16)catset[pr.second];
    u32 v;
    const u8* p = (const u8*)pr.first.data();
    u32 n = (u32)pr.first.size();
    if (n >= 2 && p[0] == 'p' && canon_num(p + 1, n - 1, v)) {
      if (v >= g_prod_by_num.size()) g_prod_by_num.resize((size_t)v + 1, 0xFFFF);
      g_prod_by_num[v] = c;
    } else {
      g_prod_other[pr.first] = c;
    }
  }
  // campaigns
  s = read_small(dir + "/campaigns.csv");
  {
    size_t pos = s.find('\n');
    pos = pos == std::string::npos ? s.size() : pos + 1;
    while (pos < s.size()) {
      size_t e = s.find('\n', pos);
      if (e == std::string::npos) e = s.size();
      Field f[2];
      if (split((const u8*)s.data() + pos, (const u8*)s.data() + e, f, 2) && f[0].n) {
        u32 idx = (u32)g_camp_ids.size();
        g_camp_ids.push_back(std::string((const char*)f[0].p, f[0].n));
        g_camp_channels.push_back(std::string((const char*)f[1].p, f[1].n));
        u32 v;
        if (f[0].n >= 3 && f[0].p[0] == 'c' && f[0].p[1] == '_' && canon_num(f[0].p + 2, f[0].n - 2, v)) {
          if (v >= g_camp_by_num.size()) g_camp_by_num.resize((size_t)v + 1, 0xFFFFFFFFu);
          g_camp_by_num[v] = idx;
        } else {
          g_camp_other[g_camp_ids.back()] = idx;
        }
      }
      pos = e + 1;
    }
  }
  g_ncamp = (u32)g_camp_ids.size();
  g_inline_clicks = g_ncamp < CAMP_MASK;
}

static i64 manifest_value(const std::string& m, const char* key) {
  size_t k = m.find(key);
  if (k == std::string::npos) die("manifest key");
  size_t c = m.find(':', k + strlen(key));
  size_t q1 = m.find('"', c);
  size_t q2 = m.find('"', q1 + 1);
  i64 v;
  u8 buf[128] = {0};
  size_t n = q2 - q1 - 1;
  if (n > 64) die("manifest ts");
  memcpy(buf, m.data() + q1 + 1, n);
  if (!parse_ts(buf, (u32)n, v)) die("manifest ts");
  return v;
}

// ---------------------------------------------------------------- records
struct ERec {  // parsed event copy, grouped by event_id
  u64 ev;
  u64 user;
  u32 ingest;  // seconds relative to g_ingest_base_s
  u32 ts;      // ms since ws, BAD_TS if the copy is invalid
  u32 price;   // purchase price, 0 for view/cart
  u32 cat;
};
struct LRec {  // event copy with a long (prefix + 32 hex) id, grouped by hash of the id
  u64 hi, lo;
  u64 user;
  u32 ingest, ts;
  u32 price;  // price | cat << 24
  u32 prefix;
  u64 pad[3];
};
struct CRec {  // valid click, grouped later by person
  u64 user;
  u64 click;
  u32 ts;    // ms since g_cs
  u32 camp;  // campaign index | inline long click id extras (see CLMARK)
  u64 clo;
};
// Long click ids (prefix + 32 hex) are carried inline instead of interned:
// click = CLMARK | hi >> 4, clo = lo, and camp bits 20..23 hold hi & 15 and
// bits 24..31 the prefix byte. Campaign indices then must fit in 20 bits.

struct PRec {  // surviving event, grouped by person
  u64 person;
  u32 ts;
  u32 pc;  // price | cat << 24
};
struct PClick {
  u64 person;
  u64 click;
  u32 ts;
  u32 camp;
  u64 clo;
};

// Bump allocator over one big lazily-committed reservation, carved per thread.
static u8* g_arena_base;
static std::atomic<size_t> g_arena_used{0};
static size_t ARENA_RESERVE = 0;
static const size_t SLAB = (size_t)32 << 20;
struct Arena {
  u8* cur = nullptr;
  u8* lim = nullptr;
  std::vector<u8*> free8k;  // recycled 8 KB scatter blocks
  void* alloc_aligned(size_t n, size_t align) {
    uintptr_t c = ((uintptr_t)cur + align - 1) & ~(uintptr_t)(align - 1);
    if (!cur || c + n > (uintptr_t)lim) {
      alloc(SLAB);  // start a fresh slab (slabs are SLAB-aligned)
      cur = lim - SLAB;
      c = (uintptr_t)cur;
    }
    cur = (u8*)(c + n);
    return (void*)c;
  }
  void* alloc(size_t n) {
    n = (n + 63) & ~(size_t)63;
    if ((size_t)(lim - cur) < n) {
      size_t take = n > SLAB ? n : SLAB;
      size_t off = g_arena_used.fetch_add(take);
      if (off + take > ARENA_RESERVE) die("arena exhausted");
      cur = g_arena_base + off;
      lim = cur + take;
    }
    void* r = cur;
    cur += n;
    return r;
  }
};

#if defined(__AVX512F__)
#include <immintrin.h>
static inline void stream64(void* dst, const void* src) {
  _mm512_stream_si512((__m512i*)dst, _mm512_load_si512(src));
}
static inline void stream_fence() { _mm_sfence(); }
#elif defined(__AVX2__)
#include <immintrin.h>
static inline void stream64(void* dst, const void* src) {
  _mm256_stream_si256((__m256i*)dst, _mm256_load_si256((const __m256i*)src));
  _mm256_stream_si256((__m256i*)dst + 1, _mm256_load_si256((const __m256i*)src + 1));
}
static inline void stream_fence() { _mm_sfence(); }
#else
static inline void stream64(void* dst, const void* src) { memcpy(dst, src, 64); }
static inline void stream_fence() { std::atomic_thread_fence(std::memory_order_seq_cst); }
#endif

// Per-thread scatter into nb buckets; each bucket is a linked list of BS-byte,
// BS-aligned blocks (64-byte header, then records). Writes go through a
// one-cache-line staging buffer per bucket and leave as 64-byte non-temporal
// stores. The per-bucket state is one word: the write pointer (64-aligned)
// with the staging count in its low 6 bits, so all heads fit in L1.
template <class T, size_t BS>
struct Buckets {
  static_assert(64 % sizeof(T) == 0, "record must divide a cache line");
  static const u32 K = 64 / sizeof(T);
  static const size_t CAPB = (BS - 64) / sizeof(T);
  struct Hdr {
    u8* next;
    u32 n;
  };
  u64* meta = nullptr;
  u8* bufs = nullptr;
  u8** last = nullptr;
  size_t nb = 0;
  Arena* arena = nullptr;
  void init(size_t n, Arena* a) {
    arena = a;
    nb = n;
    meta = (u64*)a->alloc(n * 8);
    bufs = (u8*)a->alloc(n * 64);
    last = (u8**)a->alloc(n * sizeof(u8*));
    memset(meta, 0, n * 8);
    memset(last, 0, n * sizeof(u8*));
  }
  inline u8* grow(u8* cur) {
    u8* old = cur ? cur - BS : nullptr;
    if (old) ((Hdr*)old)->n = (u32)CAPB;
    u8* blk;
    if (BS == 8192 && !arena->free8k.empty()) {
      blk = arena->free8k.back();
      arena->free8k.pop_back();
    } else {
      blk = (u8*)arena->alloc_aligned(BS, BS);
    }
    ((Hdr*)blk)->next = old;
    ((Hdr*)blk)->n = 0;
    return blk + 64;
  }
  inline void push(size_t b, const T& r) {
    u64 m = meta[b];
    u32 k = (u32)(m & 63);
    T* buf = (T*)(bufs + b * 64);
    buf[k] = r;
    if (k + 1 == K) {
      u8* cur = (u8*)(m & ~(u64)63);
      if (UNLIKELY(((uintptr_t)cur & (BS - 1)) == 0)) cur = grow(cur);
      stream64(cur, buf);
      meta[b] = (u64)(cur + 64);
    } else {
      meta[b] = m + 1;
    }
  }
  void finish() {
    for (size_t b = 0; b < nb; b++) {
      u64 m = meta[b];
      u32 k = (u32)(m & 63);
      u8* cur = (u8*)(m & ~(u64)63);
      if (k) {
        if (((uintptr_t)cur & (BS - 1)) == 0) cur = grow(cur);
        memcpy(cur, bufs + b * 64, k * sizeof(T));
        cur += k * sizeof(T);
      }
      if (!cur) continue;
      u8* blk = (u8*)(((uintptr_t)cur - 1) & ~(uintptr_t)(BS - 1));
      ((Hdr*)blk)->n = (u32)((cur - (blk + 64)) / sizeof(T));
      last[b] = blk;
      meta[b] = 0;
    }
    stream_fence();
  }
  size_t count(size_t b) const {
    size_t n = 0;
    for (u8* k = last[b]; k; k = ((Hdr*)k)->next) n += ((Hdr*)k)->n;
    return n;
  }
  // Hands bucket b's blocks to a free list (after the bucket is consumed).
  void recycle(size_t b, std::vector<u8*>& fl) {
    for (u8* k = last[b]; k;) {
      u8* nx = ((Hdr*)k)->next;
      fl.push_back(k);
      k = nx;
    }
    last[b] = nullptr;
  }
  template <class F>
  inline void each(size_t b, F&& f) const {
    for (u8* k = last[b]; k; k = ((Hdr*)k)->next) {
      const T* d = (const T*)(k + 64);
      u32 n = ((Hdr*)k)->n;
      for (u32 i = 0; i < n; i++) f(d[i]);
    }
  }
};

static int g_nt;
static int g_ebits, g_pbits;

struct ThreadState {
  int tid = 0;
  Arena arena;
  Buckets<ERec, 8192> eb;   // by hash(event_id)
  Buckets<LRec, 8192> lb;   // long event ids, by hash of the 128-bit value
  std::vector<CRec> clicks;
  Buckets<PRec, 8192> pb;   // by hash(person)
  Buckets<PClick, 8192> cb;  // by hash(person)
  std::vector<u64> camp;  // [(ncamp+1)*ndays*4]: sessions, purchasing, revenue, assisted
  struct Row {
    u64 person;
    u32 day;
    u32 sessions;
    u64 events;
    u64 revenue;
    int top;
  };
  std::vector<Row> rows;
};
static std::vector<ThreadState> g_ts;

// ---------------------------------------------------------------- parsing
// Linked IDs: concurrent open-addressing table; the slot index is the node id.
// After build, slot.pv holds the person code (the byte-wise smallest ID).
struct LSlot {
  std::atomic<u64> key;
  u64 pv;
};
static LSlot* g_lt = nullptr;
static std::atomic<u32>* g_lparent = nullptr;  // 0 = root, else parent slot + 1
static std::atomic<u64>* g_lbest = nullptr;
static u64 g_lmask = 0;
static u32 g_lshift = 64;
static bool g_lempty = true;

static void links_alloc(size_t npairs, Arena& a) {
  size_t cap = 1024;
  while (cap < npairs * 4 + 16) cap <<= 1;  // nodes <= 2 * pairs, load <= 0.5
  // Fresh arena memory is zero: keys 0 = empty, parent 0 = root, best 0 = none.
  g_lt = (LSlot*)a.alloc(cap * sizeof(LSlot));
  g_lparent = (std::atomic<u32>*)a.alloc(cap * sizeof(u32));
  g_lbest = (std::atomic<u64>*)a.alloc(cap * sizeof(u64));
  g_lmask = cap - 1;
  g_lshift = shift_of(cap);
  g_lempty = npairs == 0;
}

static inline u32 lt_insert(u64 k) {
  u64 h = slot_of(k, g_lshift);
  for (;;) {
    u64 cur = g_lt[h].key.load(std::memory_order_relaxed);
    if (cur == k) return (u32)h;
    if (cur == 0) {
      if (g_lt[h].key.compare_exchange_strong(cur, k, std::memory_order_relaxed)) return (u32)h;
      if (cur == k) return (u32)h;
    }
    h = (h + 1) & g_lmask;
  }
}

static inline u32 lt_find(u32 x) {
  for (;;) {
    u32 p = g_lparent[x].load(std::memory_order_relaxed);
    if (!p) return x;
    u32 pi = p - 1;
    u32 gp = g_lparent[pi].load(std::memory_order_relaxed);
    if (!gp) return pi;
    g_lparent[x].compare_exchange_weak(p, gp, std::memory_order_relaxed);  // path halving
    x = gp - 1;
  }
}

static inline void lt_union(u32 a, u32 b) {
  for (;;) {
    a = lt_find(a);
    b = lt_find(b);
    if (a == b) return;
    if (a < b) std::swap(a, b);  // link the larger root under the smaller: no cycles
    u32 expect = 0;
    if (g_lparent[a].compare_exchange_strong(expect, b + 1, std::memory_order_relaxed)) return;
  }
}

static void links_union(const std::vector<std::pair<u64, u64>>& pairs) {
  for (auto& pr : pairs) lt_union(lt_insert(pr.first), lt_insert(pr.second));
}

static void links_best(size_t lo, size_t hi) {
  for (size_t i = lo; i < hi; i++) {
    u64 k = g_lt[i].key.load(std::memory_order_relaxed);
    if (!k) continue;
    std::atomic<u64>& b = g_lbest[lt_find((u32)i)];
    u64 cur = b.load(std::memory_order_relaxed);
    while (cur == 0 || code_cmp(k, cur) < 0)
      if (b.compare_exchange_weak(cur, k, std::memory_order_relaxed)) break;
  }
}

static void links_finish(size_t lo, size_t hi) {
  for (size_t i = lo; i < hi; i++)
    if (g_lt[i].key.load(std::memory_order_relaxed))
      g_lt[i].pv = g_lbest[lt_find((u32)i)].load(std::memory_order_relaxed);
}

static inline void person_prefetch(u64 user) {
  if (!g_lempty) __builtin_prefetch(&g_lt[slot_of(user, g_lshift)]);
}

static inline u64 person_of(u64 user) {
  if (g_lempty) return user;
  u64 h = slot_of(user, g_lshift);
  for (;;) {
    u64 k = g_lt[h].key.load(std::memory_order_relaxed);
    if (k == user) return g_lt[h].pv;
    if (k == 0) return user;
    h = (h + 1) & g_lmask;
  }
}

static inline bool parse_price(const Field& pr, u32& out) {
  u32 i = 0;
  while (i < pr.n && pr.p[i] == '0') i++;
  if (pr.n == 0 || pr.n - i > 8) return false;
  u32 v = 0;
  for (u32 k = i; k < pr.n; k++) {
    u8 d = (u8)(pr.p[k] - '0');
    if (d > 9) return false;
    v = v * 10 + d;
  }
  if (v < 1 || v > 10000000) return false;
  out = v;
  return true;
}

#ifndef SS_PFD
#define SS_PFD 16
#endif
#ifndef SS_STAGE
#define SS_STAGE 0
#endif
static bool g_fast = false;
static u32 g_psh = 13;
static inline bool event_index(const u8* p, u32 n, u64& idx);
static void odd_push(int tid, const ERec& r);
static thread_local u64 g_sink;
static void parse_events(const Chunk& c0, ThreadState& t) {
  Chunk c = c0;
  if (!resolve_chunk(c0, c.s, c.e)) return;
  const i64 ws = g_ws, we = g_we;
  const u32 eshift = 64 - g_ebits;
#if SS_STAGE == 4
  g_sink += c.e - c.s;
  return;
#endif
  scan_lines<7>(c.s, c.e, [&](const Field* f) __attribute__((always_inline)) {
#if SS_STAGE == 1 || SS_STAGE == 3
    g_sink += f[0].n + f[6].n;
    return;
#endif
    if (UNLIKELY(f[0].n == 0)) return;
    ERec r;
    u64 idx = 0, lhi = 0, llo = 0;
    bool fast_id = g_fast && event_index(f[0].p, f[0].n, idx);
    bool long_id = !g_fast && f[0].n == 33 && hex32(f[0].p + 1, lhi, llo);
    r.ev = fast_id ? idx : (long_id ? 0 : code_of(f[0].p, f[0].n));
    i64 ing;
    if (UNLIKELY(!parse_ingest(f[3].p, f[3].n, ing))) ing = INT64_MAX / 2;
    i64 is = ing / 1000 - g_ingest_base_s;
    r.ingest = is < 0 ? 0 : (is > 0xFFFFFFFFLL ? 0xFFFFFFFFu : (u32)is);
    r.ts = BAD_TS;
    r.user = 0;
    r.price = 0;
    r.cat = 0;
    i64 ts;
    bool valid = f[1].n && f[5].n && parse_ts(f[2].p, f[2].n, ts) && ts >= ws && ts < we && ing - ts <= DAY_MS;
    if (valid) {
      const Field& ty = f[4];
      if (ty.n == 4 && (memcmp(ty.p, "view", 4) == 0 || memcmp(ty.p, "cart", 4) == 0)) {
      } else if (ty.n == 8 && memcmp(ty.p, "purchase", 8) == 0) {
        valid = parse_price(f[6], r.price);
      } else {
        valid = false;
      }
    }
    if (valid) {
      r.ts = (u32)(ts - ws);
      r.user = code_of(f[1].p, f[1].n);
      r.cat = r.price ? product_cat(f[5]) : 0;
      r.price |= r.cat << 24;
    } else {
      r.price = 0;
    }
#if SS_STAGE == 2
    g_sink += r.ev + r.ts + r.user + r.ingest;
    return;
#endif
    if (g_fast) {
      if (LIKELY(fast_id))
        t.eb.push(r.ev >> g_psh, r);
      else
        odd_push(t.tid, r);
      return;
    }
    if (UNLIKELY(long_id)) {
      LRec l;
      l.hi = lhi;
      l.lo = llo;
      l.user = r.user;
      l.ingest = r.ingest;
      l.ts = r.ts;
      l.price = r.price;
      l.prefix = f[0].p[0];
      l.pad[0] = l.pad[1] = l.pad[2] = 0;
      t.lb.push(hb(lhi ^ (llo * 0xC2B2AE3D27D4EB4FULL)) >> eshift, l);
      return;
    }
    t.eb.push(hb(r.ev) >> eshift, r);
  });
}

static void parse_clicks(const Chunk& c0, ThreadState& t) {
#if SS_STAGE >= 3
  return;
#endif
  Chunk c = c0;
  if (!resolve_chunk(c0, c.s, c.e)) return;
  const i64 cs = g_cs, we = g_we;
  scan_lines<4>(c.s, c.e, [&](const Field* f) __attribute__((always_inline)) {
    if (f[0].n == 0 || f[1].n == 0) return;
    i64 ts;
    if (!parse_ts(f[3].p, f[3].n, ts) || ts < cs || ts >= we) return;
    u32 camp = campaign_idx(f[2]);
    if (camp == 0xFFFFFFFFu) return;
    CRec r;
    r.user = code_of(f[1].p, f[1].n);
    u64 chi, clo;
    if (g_inline_clicks && f[0].n == 33 && hex32(f[0].p + 1, chi, clo)) {
      r.click = CLMARK | (chi >> 4);
      r.clo = clo;
      camp |= (u32)(chi & 15) << 20 | (u32)f[0].p[0] << 24;
    } else {
      r.click = code_of(f[0].p, f[0].n, 1);
      r.clo = 0;
    }
    r.ts = (u32)(ts - cs);
    r.camp = camp;
    t.clicks.push_back(r);
  });
}

// After parsing: persons for this thread's clicks (lookups only, prefetched),
// then the scatter by person.
static void route_clicks(ThreadState& t) {
  const u32 pshift = 64 - g_pbits;
  const size_t D = SS_PFD, n = t.clicks.size();
  for (size_t i = 0; i < n && i < D; i++) person_prefetch(t.clicks[i].user);
  for (size_t i = 0; i < n; i++) {
    if (i + D < n) person_prefetch(t.clicks[i + D].user);
    t.clicks[i].user = person_of(t.clicks[i].user);
  }
  for (size_t i = 0; i < n; i++) {
    const CRec& c = t.clicks[i];
    PClick o;
    o.person = c.user;
    o.click = c.click;
    o.clo = c.clo;
    o.ts = c.ts;
    o.camp = c.camp;

    t.cb.push(hb(o.person) >> pshift, o);
  }
}

static void parse_links(const Chunk& c0, std::vector<std::pair<u64, u64>>& pairs) {
  Chunk c = c0;
  if (!resolve_chunk(c0, c.s, c.e)) return;
  scan_lines<2>(c.s, c.e, [&](const Field* f) {
    if (!f[0].n || !f[1].n) return;
    pairs.push_back({code_of(f[0].p, f[0].n), code_of(f[1].p, f[1].n)});
  });
}

// ---------------------------------------------------------------- links
struct U64Map {  // open addressing, interleaved key/value, key 0 = empty
  struct KV {
    u64 k, v;
  };
  std::vector<KV> t;
  u64 mask = 0;
  void init(size_t n) {
    size_t cap = 16;
    while (cap < n * 2) cap <<= 1;
    t.assign(cap, KV{0, 0});
    mask = cap - 1;
  }
  inline u64* find_or_insert(u64 key, bool& inserted) {
    u64 i = mix64(key) & mask;
    for (;;) {
      if (t[i].k == key) {
        inserted = false;
        return &t[i].v;
      }
      if (t[i].k == 0) {
        t[i].k = key;
        inserted = true;
        return &t[i].v;
      }
      i = (i + 1) & mask;
    }
  }
  inline void prefetch(u64 key) const { __builtin_prefetch(&t[mix64(key) & mask]); }
  inline const u64* find(u64 key) const {
    u64 i = mix64(key) & mask;
    const KV* d = t.data();
    for (;;) {
      if (d[i].k == key) return &d[i].v;
      if (d[i].k == 0) return nullptr;
      i = (i + 1) & mask;
    }
  }
};

// ---------------------------------------------------------------- phase 3: dedup
struct DSlot {  // unused; kept for the worker's scratch type
  u64 key;
  const ERec* r;
};
// Dedup one event bucket. A 2-bit saturating counter per hash slot (L1/L2
// resident) marks event ids seen once; those are unique and skip any table.
// Ids whose counter reached 2 (true duplicates or hash collisions) are
// resolved exactly in a small open-addressing table keyed by the full id.
struct Surv {
  u64 user;
  u32 ts;
  u32 pc;
};
static void dedup_bucket(int b, ThreadState& t, std::vector<DSlot>&, std::vector<const ERec*>&) {
  static thread_local std::vector<u64> bm;
  static thread_local std::vector<const ERec*> ctab;
  static thread_local std::vector<Surv> sv;
  static thread_local std::vector<u64> users;
  size_t n = 0, ln = 0;
  for (int i = 0; i < g_nt; i++) {
    n += g_ts[i].eb.count(b);
    ln += g_ts[i].lb.count(b);
  }
  if (!n && !ln) return;
  size_t slots = 256;
  while (slots < n * 4) slots <<= 1;  // 2 bits each: n bytes
  if (bm.size() < slots / 32) bm.resize(slots / 32);
  u64* B = bm.data();
  memset(B, 0, slots / 4);
  const u32 bsh = shift_of(slots);
  for (int i = 0; i < g_nt; i++)
    g_ts[i].eb.each(b, [&](const ERec& r) {
      u64 h = slot_of(r.ev, bsh);
      u64& w = B[h >> 5];
      u32 sh = (u32)(h & 31) * 2;
      u64 c = (w >> sh) & 3;
      w += (u64)(c < 2) << sh;
    });
  if (sv.size() < n + ln) sv.resize(n + ln);
  Surv* S = sv.data();
  size_t m = 0, nc = 0;
  // Candidate table sized lazily; most buckets have ~3% candidates.
  size_t ccap = 64;
  while (ccap < (n / 8) * 2 + 64) ccap <<= 1;
  if (ctab.size() < ccap) ctab.resize(ccap);
  const ERec** C = ctab.data();
  memset((void*)C, 0, ccap * sizeof(const ERec*));
  u64 cmask = ccap - 1;
  u32 csh = shift_of(ccap);
  bool overflow = false;
  for (int i = 0; i < g_nt; i++)
    g_ts[i].eb.each(b, [&](const ERec& r) {
      u64 h = slot_of(r.ev, bsh);
      u64 c = (B[h >> 5] >> ((h & 31) * 2)) & 3;
      if (LIKELY(c == 1)) {
        if (r.ts != BAD_TS) S[m++] = {r.user, r.ts, r.price};
        return;
      }
      if (UNLIKELY(nc * 2 >= ccap)) {
        overflow = true;
        return;
      }
      u64 j = slot_of(r.ev, csh);
      for (;;) {
        const ERec* x = C[j];
        if (!x) {
          C[j] = &r;
          nc++;
          break;
        }
        if (x->ev == r.ev) {
          if (r.ingest < x->ingest) C[j] = &r;
          break;
        }
        j = (j + 1) & cmask;
      }
    });
  if (UNLIKELY(overflow)) {
    // Too many candidates for the small table: redo the candidates with a big one.
    ccap = 64;
    while (ccap < n * 2) ccap <<= 1;
    if (ctab.size() < ccap) ctab.resize(ccap);
    C = ctab.data();
    memset((void*)C, 0, ccap * sizeof(const ERec*));
    cmask = ccap - 1;
    csh = shift_of(ccap);
    nc = 0;
    for (int i = 0; i < g_nt; i++)
      g_ts[i].eb.each(b, [&](const ERec& r) {
        u64 h = slot_of(r.ev, bsh);
        if (((B[h >> 5] >> ((h & 31) * 2)) & 3) == 1) return;
        u64 j = slot_of(r.ev, csh);
        for (;;) {
          const ERec* x = C[j];
          if (!x) {
            C[j] = &r;
            break;
          }
          if (x->ev == r.ev) {
            if (r.ingest < x->ingest) C[j] = &r;
            break;
          }
          j = (j + 1) & cmask;
        }
      });
  }
  for (size_t j = 0; j < ccap; j++) {
    const ERec* x = C[j];
    if (x && x->ts != BAD_TS) S[m++] = {x->user, x->ts, x->price};
  }
  if (ln) {  // long ids: exact dedup on (prefix, 128-bit value)
    static thread_local std::vector<const LRec*> ltab;
    size_t lcap = 64;
    while (lcap < ln * 2) lcap <<= 1;
    if (ltab.size() < lcap) ltab.resize(lcap);
    const LRec** L = ltab.data();
    memset((void*)L, 0, lcap * sizeof(const LRec*));
    const u64 lmask = lcap - 1;
    for (int i = 0; i < g_nt; i++)
      g_ts[i].lb.each(b, [&](const LRec& r) {
        u64 j = (ht(r.hi ^ (r.lo * 0x9E3779B97F4A7C15ULL) ^ r.prefix)) & lmask;
        for (;;) {
          const LRec* x = L[j];
          if (!x) {
            L[j] = &r;
            break;
          }
          if (x->hi == r.hi && x->lo == r.lo && x->prefix == r.prefix) {
            if (r.ingest < x->ingest) L[j] = &r;
            break;
          }
          j = (j + 1) & lmask;
        }
      });
    for (size_t j = 0; j < lcap; j++) {
      const LRec* x = L[j];
      if (x && x->ts != BAD_TS) S[m++] = {x->user, x->ts, x->price};
    }
  }
  // Person lookups only (prefetched), then the scatter by person.
  if (users.size() < m) users.resize(m);
  const size_t D = SS_PFD;
  for (size_t i = 0; i < m && i < D; i++) person_prefetch(S[i].user);
  for (size_t i = 0; i < m; i++) {
    if (i + D < m) person_prefetch(S[i + D].user);
    users[i] = person_of(S[i].user);
  }
  const u32 pshift = 64 - g_pbits;
  for (size_t i = 0; i < m; i++) {
    PRec o;
    o.person = users[i];
    o.ts = S[i].ts;
    o.pc = S[i].pc;
    t.pb.push(hb(o.person) >> pshift, o);
  }
  // Recycle the bucket's blocks only now: candidate pointers referenced them.
  for (int i = 0; i < g_nt; i++) {
    g_ts[i].eb.recycle(b, t.arena.free8k);
    g_ts[i].lb.recycle(b, t.arena.free8k);
  }
}


// ---------------------------------------------------------------- phase 4: sessions
struct EV {
  u32 ts;
  u32 pc;  // price | cat << 24
};
struct TRec {
  u32 ts;
  u32 pc;
  u32 e;
  u32 pad;
};
struct CK {
  u32 ts;
  u32 camp;
  u64 click;
  u64 clo;
};
// Byte-wise order of two click ids (ties on equal ts only).
static inline const char* ck_text(const CK& c, char* buf, u32& len) {
  if ((c.click & CLMARK) == CLMARK) {
    u64 hi = ((c.click & ~CLMARK) << 4) | ((c.camp >> 20) & 15);
    u8 bytes[16];
    memcpy(bytes, &hi, 8);
    memcpy(bytes + 8, &c.clo, 8);
    buf[0] = (char)(c.camp >> 24);
    for (int i = 0; i < 16; i++) {
      buf[1 + 2 * i] = "0123456789abcdef"[bytes[i] >> 4];
      buf[2 + 2 * i] = "0123456789abcdef"[bytes[i] & 15];
    }
    len = 33;
    return buf;
  }
  return decode(c.click, buf, len);
}
static inline bool ck_less(const CK& a, const CK& b) {
  if (a.ts != b.ts) return a.ts < b.ts;
  if (!((a.click | b.click) & CLMARK) || ((a.click & CLMARK) != CLMARK && (b.click & CLMARK) != CLMARK))
    return code_cmp(a.click, b.click) < 0;
  char ba[40], bb[40];
  u32 la, lb;
  const char* sa = ck_text(a, ba, la);
  const char* sb = ck_text(b, bb, lb);
  int r = memcmp(sa, sb, la < lb ? la : lb);
  if (r) return r < 0;
  return la < lb;
}
struct PEnt {
  u64 person;
  u32 cnt, off, ccnt, coff;
};

struct Scratch {
  std::vector<u32> slot;  // hash -> entry index+1
  std::vector<TRec> t1, t2;
  std::vector<u32> cidx;
  std::vector<PEnt> ents;
  std::vector<EV> ev;
  std::vector<CK> ck;
  std::vector<u32> campcnt, since;
  std::vector<u32> touched;
  std::vector<u8> sessday;
  struct Span {
    u32 camp, a, b;
  };
  std::vector<Span> spans;
  std::vector<std::pair<u32, u64>> daycats;
};

static inline u32 day_of(u32 ts_rel) {  // day index of a ws-relative ts
  return (u32)((g_ws + (i64)ts_rel) / DAY_MS - g_wsday);
}

static void process_core(ThreadState& t, Scratch& s, u64 person, const EV* ev, u32 n, const CK* ck, u32 m);
static void process_person(ThreadState& t, Scratch& s, const PEnt& pe) {
  CK* ck = s.ck.data() + pe.coff;
  u32 m = pe.ccnt;
  if (m > 1)
    std::sort(ck, ck + m, ck_less);
  process_core(t, s, pe.person, s.ev.data() + pe.off, pe.cnt, ck, m);
}
// Sessionize, credit and aggregate one person: events sorted by ts, clicks by (ts, click id).
static void process_core(ThreadState& t, Scratch& s, u64 person, const EV* ev, u32 n, const CK* ck, u32 m) {
  const u32 nd = (u32)g_ndays;
  u64* camp = t.camp.data();
  s.sessday.clear();
  s.spans.clear();
  s.touched.clear();
  s.daycats.clear();
  u32 lo = 0, hi = 0, sidx = 0;
  int curday = -1;
  u32 dsess = 0;
  u64 devents = 0, drev = 0;
  auto flush = [&]() {
    if (curday < 0) return;
    int top = -1;
    u64 bestv = 0;
    for (auto& pc : s.daycats)
      if (pc.second > bestv || (pc.second == bestv && (int)pc.first < top)) {
        bestv = pc.second;
        top = (int)pc.first;
      }
    t.rows.push_back({person, (u32)curday, dsess, devents, drev, top});
    s.daycats.clear();
    dsess = 0;
    devents = 0;
    drev = 0;
  };
  u32 i = 0;
  while (i < n) {
    u32 S = ev[i].ts;
    u32 prev = S;
    u32 j = i;
    u64 rev = 0;
    for (; j < n && ev[j].ts - prev <= (u32)GAP_MS; j++) {
      prev = ev[j].ts;
      rev += ev[j].pc & 0xFFFFFF;
    }
    u32 day = day_of(S);
    if ((int)day != curday) {
      flush();
      curday = (int)day;
    }
    dsess++;
    devents += j - i;
    drev += rev;
    if (rev) {
      for (u32 k = i; k < j; k++) {
        u32 price = ev[k].pc & 0xFFFFFF, cat = ev[k].pc >> 24;
        if (!price) continue;
        bool found = false;
        for (auto& pc : s.daycats)
          if (pc.first == cat) {
            pc.second += price;
            found = true;
            break;
          }
        if (!found) s.daycats.push_back({cat, price});
      }
    }
    // clicks: window [S - 7d, S] in click base is [S, S + WEEK]
    u64 Sc = (u64)S + WEEK_MS;
    while (hi < m && ck[hi].ts <= Sc) {
      u32 c = ck[hi].camp & CAMP_MASK;
      if (s.campcnt[c]++ == 0) {
        s.since[c] = sidx;
        s.touched.push_back(c);
      }
      hi++;
    }
    while (lo < hi && ck[lo].ts < S) {
      u32 c = ck[lo].camp & CAMP_MASK;
      if (--s.campcnt[c] == 0) s.spans.push_back({c, s.since[c], sidx});
      lo++;
    }
    u32 credit = g_ncamp;
    if (lo < hi) {
      u32 k = hi - 1;
      while (k > lo && ck[k - 1].ts == ck[k].ts) k--;
      credit = ck[k].camp & CAMP_MASK;
    }
    u64* cc = camp + ((size_t)credit * nd + day) * 4;
    cc[0]++;
    if (rev) cc[1]++;
    cc[2] += rev;
    s.sessday.push_back((u8)day);
    sidx++;
    i = j;
  }
  flush();
  for (u32 c : s.touched) {
    if (s.campcnt[c]) {
      s.spans.push_back({c, s.since[c], sidx});
      s.campcnt[c] = 0;
    }
  }
  if (s.spans.empty()) return;
  // first[d] = index of the first session on day >= d (sessions are in day order).
  u32 first[257];
  {
    u32 k = 0;
    for (u32 d = 0; d <= nd; d++) {
      while (k < sidx && s.sessday[k] < d) k++;
      first[d] = k;
    }
  }
  for (auto& sp : s.spans) {
    if (sp.a >= sp.b) continue;
    u64* base = camp + (size_t)sp.camp * nd * 4 + 3;
    for (u32 d = s.sessday[sp.a], dl = s.sessday[sp.b - 1]; d <= dl; d++) {
      u32 lo = std::max(sp.a, first[d]), hi = std::min(sp.b, first[d + 1]);
      if (hi > lo) base[(size_t)d * 4] += hi - lo;
    }
  }
}

#ifdef SS_PROF
#include <x86intrin.h>
static std::atomic<u64> g_pt[6];
#define PT(i) { u64 _n = __rdtsc(); g_pt[i] += _n - _t; _t = _n; }
#else
#define PT(i)
#endif
static inline const CK* clicks_of(u64 p, u32& m);
static void person_bucket(int b, ThreadState& t, Scratch& s) {
#ifdef SS_PROF
  u64 _t = __rdtsc();
#endif
  size_t n = 0, m = 0;
  for (int i = 0; i < g_nt; i++) {
    n += g_ts[i].pb.count(b);
    m += g_ts[i].cb.count(b);
  }
  if (!n) return;
  size_t cap = 16;
  while (cap < n * 2) cap <<= 1;
  if (s.slot.size() < cap) s.slot.resize(cap);
  std::fill(s.slot.begin(), s.slot.begin() + cap, 0);
  u64 mask = cap - 1;
  const u32 tsh = shift_of(cap);
  s.ents.clear();
  if (s.t1.size() < n) {
    s.t1.resize(n);
    s.t2.resize(n);
  }
  TRec* t1 = s.t1.data();
  TRec* t2 = s.t2.data();
  size_t k = 0;
  for (int i = 0; i < g_nt; i++)
    g_ts[i].pb.each(b, [&](const PRec& r) {
      u64 h = slot_of(r.person, tsh);
      u32 e;
      for (;;) {
        e = s.slot[h];
        if (!e) {
          s.ents.push_back({r.person, 1, 0, 0, 0});
          e = (u32)s.ents.size();
          s.slot[h] = e;
          break;
        }
        if (s.ents[e - 1].person == r.person) {
          s.ents[e - 1].cnt++;
          break;
        }
        h = (h + 1) & mask;
      }
      t1[k++] = {r.ts, r.pc, e - 1, 0};
    });
  PT(0);
#ifndef SS_NORADIX
  // LSD radix sort by ts (3 x 11 bits), stable.
  {
    u32 hist[3][2048];
    memset(hist, 0, sizeof(hist));
    for (size_t i = 0; i < n; i++) {
      u32 x = t1[i].ts;
      hist[0][x & 2047]++;
      hist[1][(x >> 11) & 2047]++;
      hist[2][x >> 22]++;
    }
    for (int p = 0; p < 3; p++) {
      u32 sum = 0;
      for (int i = 0; i < 2048; i++) {
        u32 c = hist[p][i];
        hist[p][i] = sum;
        sum += c;
      }
    }
    for (size_t i = 0; i < n; i++) t2[hist[0][t1[i].ts & 2047]++] = t1[i];
    for (size_t i = 0; i < n; i++) t1[hist[1][(t2[i].ts >> 11) & 2047]++] = t2[i];
    for (size_t i = 0; i < n; i++) t2[hist[2][t1[i].ts >> 22]++] = t1[i];
  }
#else
  t2 = t1;  // grouped by person below, then sorted per person
#endif
  PT(1);
  auto lookup = [&](u64 p) -> u32 {
    u64 h = slot_of(p, tsh);
    for (;;) {
      u32 e = s.slot[h];
      if (!e) return 0;
      if (s.ents[e - 1].person == p) return e;
      h = (h + 1) & mask;
    }
  };
  if (s.cidx.size() < m) s.cidx.resize(m);
  size_t ci = 0;
  if (!g_fast)
  for (int i = 0; i < g_nt; i++)
    g_ts[i].cb.each(b, [&](const PClick& c) {
      u32 e = lookup(c.person);
      s.cidx[ci++] = e;
      if (e) s.ents[e - 1].ccnt++;
    });
  u32 off = 0, coff = 0;
  for (auto& e : s.ents) {
    e.off = off;
    off += e.cnt;
    e.coff = coff;
    coff += e.ccnt;
    e.cnt = 0;
    e.ccnt = 0;
  }
  if (s.ev.size() < n) s.ev.resize(n);
  if (s.ck.size() < coff) s.ck.resize(coff);
  for (size_t i = 0; i < n; i++) {
    PEnt& e = s.ents[t2[i].e];
    s.ev[e.off + e.cnt++] = {t2[i].ts, t2[i].pc};
  }
  ci = 0;
  if (!g_fast)
  for (int i = 0; i < g_nt; i++)
    g_ts[i].cb.each(b, [&](const PClick& c) {
      u32 ei = s.cidx[ci++];
      if (!ei) return;
      PEnt& e = s.ents[ei - 1];
      s.ck[e.coff + e.ccnt++] = {c.ts, c.camp, c.click, c.clo};
    });
  PT(2);
#ifdef SS_NORADIX
  for (auto& e : s.ents) {
    EV* ev = s.ev.data() + e.off;
    u32 c = e.cnt;
    if (c <= 1) continue;
    if (c <= 32) {
      for (u32 i = 1; i < c; i++) {
        EV x = ev[i];
        u32 j = i;
        while (j > 0 && ev[j - 1].ts > x.ts) {
          ev[j] = ev[j - 1];
          j--;
        }
        ev[j] = x;
      }
    } else {
      std::sort(ev, ev + c, [](const EV& a, const EV& b) { return a.ts < b.ts; });
    }
  }
#endif
  if (g_fast) {
    for (auto& e : s.ents) {
      u32 cm;
      const CK* ck = clicks_of(e.person, cm);
      process_core(t, s, e.person, s.ev.data() + e.off, e.cnt, ck, cm);
    }
  } else {
    for (auto& e : s.ents) process_person(t, s, e);
  }
  PT(3);
}


// ================================================================ fast engine
// The generator writes event ids as 'e' + base36(scramble(index, key)) with
// a fixed invertible 40-bit mix and one secret key per dataset, and gives each
// person consecutive indices in time order. We recover the key from a sample,
// dedup by index in dense per-range slot arrays, and read each range in index
// order, which is person-then-time order. Anything that does not fit (ids that
// do not decode, persons split across ranges or out of order) takes an exact
// general path, so the output never depends on these assumptions.
static const u64 KM = (1ULL << 40) - 1;
static const u64 KA = 0x9E3779B97F4BULL, KB = 0xD6E8FEB86659ULL;
static u64 inv40(u64 a) {
  u64 x = a;
  for (int i = 0; i < 6; i++) x *= 2 - a * x;
  return x & KM;
}
static u64 KAI, KBI;
static inline u64 unmix40(u64 y) {  // undo the key-independent tail of the mix
  u64 x = (y * KBI) & KM;
  return x ^ (x >> 17) ^ (x >> 34);
}
// Canonical base36 (no leading zero unless one digit), at most 8 digits, < 2^40.
static inline bool b36_value(const u8* p, u32 k, u64& v) {
  if (k - 1 >= 8) return false;
  if (k > 1 && p[0] == '0') return false;
  u64 x = 0;
  u8 bad = 0;
  for (u32 i = 0; i < k; i++) {
    u8 d = B36T[p[i]];
    bad |= d;
    x = x * 36 + (d & 63);
  }
  if (bad & 0x80) return false;
  if (x > KM) return false;
  v = x;
  return true;
}
static u64 g_ekey = 0, g_nmax = 0;
static inline bool event_index(const u8* p, u32 n, u64& idx) {
  u64 v;
  if (n < 2 || p[0] != 'e' || !b36_value(p + 1, n - 1, v)) return false;
  u64 i = ((unmix40(v) ^ g_ekey) * KAI) & KM;
  if (i >= g_nmax) return false;
  idx = i;
  return true;
}

// Sample: the unmixed values of decodable event ids from the starts of files.
static std::vector<u64> g_samp;
static double g_avgline = 80;
static void key_sample(const std::vector<Chunk>& ech) {
  std::vector<u8> buf;
  size_t lines = 0, bytes = 0;
  int files = 0;
  const int* lastfd = nullptr;
  for (auto& c : ech) {
    if (c.off != 0 || files >= 8) continue;
    (void)lastfd;
    files++;
    size_t n = std::min<u64>(c.fsize, 256 << 10);
    buf.assign(n + 128, 0);
    ssize_t r = pread(c.fd, buf.data(), n, 0);
    if (r <= 0) continue;
    n = (size_t)r;
    const u8* s = (const u8*)memchr(buf.data(), '\n', n);
    if (!s) continue;
    s++;
    const u8* e = buf.data() + n;
    while (e > s && e[-1] != '\n') e--;  // whole lines only
    if (e <= s) continue;
    bytes += (size_t)(e - s);
    scan_lines<7>(s, e, [&](const Field* f) {
      lines++;
      u64 v;
      if (f[0].n >= 2 && f[0].p[0] == 'e' && b36_value(f[0].p + 1, f[0].n - 1, v)) g_samp.push_back(unmix40(v));
    });
  }
  if (lines) g_avgline = (double)bytes / (double)lines;
  std::sort(g_samp.begin(), g_samp.end());
  g_samp.erase(std::unique(g_samp.begin(), g_samp.end()), g_samp.end());
}
// Each thread searches a slice of index hypotheses for sample[0].
static std::mutex g_key_mu;
static std::vector<u64> g_keys_found;
static void key_search(int t, int nt) {
  if (g_samp.size() < 64) return;
  const u64 y1 = g_samp[0], y2 = g_samp[g_samp.size() / 2], y3 = g_samp.back();
  const u64 N = g_nmax;
  u64 lo = N * t / nt, hi = N * (t + 1) / nt;
  for (u64 i = lo; i < hi; i++) {
    u64 key = y1 ^ ((i * KA) & KM);
    u64 a = ((y2 ^ key) * KAI) & KM;
    u64 b = ((y3 ^ key) * KAI) & KM;
    if (UNLIKELY((a < N) & (b < N))) {
      bool ok = true;
      for (u64 y : g_samp)
        if ((((y ^ key) * KAI) & KM) >= N) {
          ok = false;
          break;
        }
      if (ok) {
        std::lock_guard<std::mutex> g(g_key_mu);
        g_keys_found.push_back(key);
      }
    }
  }
}

// Click index: per person bucket, each person's clicks sorted by (ts, click id).
struct CkIdx {
  u64* keys = nullptr;
  u32* off = nullptr;
  u32* cnt = nullptr;
  CK* data = nullptr;
  u64 mask = 0;
  u32 shift = 64;
};
static std::vector<CkIdx> g_ckidx;
static void build_ck_bucket(int b, ThreadState& t) {
  size_t m = 0;
  for (int i = 0; i < g_nt; i++) m += g_ts[i].cb.count(b);
  CkIdx& X = g_ckidx[b];
  if (!m) return;
  size_t cap = 16;
  while (cap < m * 2) cap <<= 1;
  X.keys = (u64*)t.arena.alloc(cap * 8);
  X.off = (u32*)t.arena.alloc(cap * 4);
  X.cnt = (u32*)t.arena.alloc(cap * 4);
  memset(X.keys, 0, cap * 8);
  memset(X.cnt, 0, cap * 4);
  X.mask = cap - 1;
  X.shift = shift_of(cap);
  X.data = (CK*)t.arena.alloc(m * sizeof(CK));
  auto find = [&](u64 p) -> u64 {
    u64 h = slot_of(p, X.shift);
    for (;;) {
      if (X.keys[h] == p || X.keys[h] == 0) return h;
      h = (h + 1) & X.mask;
    }
  };
  for (int i = 0; i < g_nt; i++)
    g_ts[i].cb.each(b, [&](const PClick& c) {
      u64 h = find(c.person);
      X.keys[h] = c.person;
      X.cnt[h]++;
    });
  u32 o = 0;
  for (size_t h = 0; h <= X.mask; h++) {
    X.off[h] = o;
    o += X.cnt[h];
    X.cnt[h] = 0;
  }
  for (int i = 0; i < g_nt; i++)
    g_ts[i].cb.each(b, [&](const PClick& c) {
      u64 h = find(c.person);
      X.data[X.off[h] + X.cnt[h]++] = {c.ts, c.camp, c.click, c.clo};
    });
  for (size_t h = 0; h <= X.mask; h++) {
    u32 k = X.cnt[h];
    if (k > 1)
      std::sort(X.data + X.off[h], X.data + X.off[h] + k, ck_less);
  }
}
static inline const CK* clicks_of(u64 p, u32& m) {
  const CkIdx& X = g_ckidx[hb(p) >> (64 - g_pbits)];
  m = 0;
  if (!X.keys) return nullptr;
  u64 h = slot_of(p, X.shift);
  for (;;) {
    u64 k = X.keys[h];
    if (k == p) {
      m = X.cnt[h];
      return X.data + X.off[h];
    }
    if (k == 0) return nullptr;
    h = (h + 1) & X.mask;
  }
}

// Runs: maximal stretches of one person in index order with nondecreasing ts.
struct Run {
  u64 person;
  u64 off;  // into the thread's run events
  u32 n;
  u32 pad;
};
struct FastTS {
  std::vector<Run> runs;
  std::vector<EV> rev;
  std::vector<ERec> odd;  // events whose id does not decode
  u64 ckey[512];
  u64 cval[512];
};
static std::vector<FastTS> g_fts;
static void odd_push(int tid, const ERec& r) { g_fts[tid].odd.push_back(r); }
// Events with undecodable ids: exact dedup by id code, each survivor its own run.
static void odd_resolve() {
  std::unordered_map<u64, ERec> best;
  for (auto& f : g_fts)
    for (const ERec& r : f.odd) {
      auto it = best.find(r.ev);
      if (it == best.end())
        best.emplace(r.ev, r);
      else if (r.ingest < it->second.ingest)
        it->second = r;
    }
  FastTS& f0 = g_fts[0];
  for (auto& kv : best) {
    const ERec& r = kv.second;
    if (r.ts == BAD_TS) continue;
    f0.runs.push_back({person_of(r.user), (u64)f0.rev.size(), 1, 0});
    f0.rev.push_back({r.ts, r.price});
  }
}
static inline u64 person_cached(FastTS& f, u64 user) {
  u32 h = (u32)(hb(user) >> 55);
  if (f.ckey[h] == user) return f.cval[h];
  u64 p = person_of(user);
  f.ckey[h] = user;
  f.cval[h] = p;
  return p;
}
static inline void run_add(FastTS& f, u64 p, u32 ts, u32 pc, u64& curp, u32& lastts) {
  if (p != curp || ts < lastts || f.runs.empty()) {
    f.runs.push_back({p, (u64)f.rev.size(), 0, 0});
    curp = p;
  }
  f.rev.push_back({ts, pc});
  f.runs.back().n++;
  lastts = ts;
}
static void fast_partition(int q, ThreadState& t, FastTS& f) {
  static thread_local std::vector<ERec> tab;
  static thread_local std::vector<u64> occ;
  const size_t slots = (size_t)1 << g_psh;
  if (tab.size() < slots) {
    tab.resize(slots);
    occ.resize(slots / 64);
  }
  ERec* T = tab.data();
  u64* O = occ.data();
  memset(O, 0, slots / 8);
  const u64 base = (u64)q << g_psh;
  bool any = false;
  for (int i = 0; i < g_nt; i++)
    g_ts[i].eb.each(q, [&](const ERec& r) {
      u64 s = r.ev - base;
      u64 bit = 1ULL << (s & 63);
      any = true;
      if (!(O[s >> 6] & bit)) {
        O[s >> 6] |= bit;
        T[s] = r;
      } else if (r.ingest < T[s].ingest) {
        T[s] = r;
      }
    });
  for (int i = 0; i < g_nt; i++) g_ts[i].eb.recycle(q, t.arena.free8k);
  if (!any) return;
  u64 curp = ~0ULL;
  u32 lastts = 0;
  bool first = true;
  for (size_t w = 0; w < slots / 64; w++) {
    u64 bits = O[w];
    while (bits) {
      size_t s = w * 64 + (size_t)__builtin_ctzll(bits);
      bits &= bits - 1;
      const ERec& r = T[s];
      if (r.ts == BAD_TS) continue;
      u64 p = person_cached(f, r.user);
      if (first) {  // a new partition always starts a new run
        f.runs.push_back({p, (u64)f.rev.size(), 0, 0});
        curp = p;
        first = false;
        f.rev.push_back({r.ts, r.price});
        f.runs.back().n++;
        lastts = r.ts;
        continue;
      }
      run_add(f, p, r.ts, r.price, curp, lastts);
    }
  }
}
// Count runs per person in a concurrent table.
static std::atomic<u64>* g_rk = nullptr;
static std::atomic<u32>* g_rc = nullptr;
static u64 g_rmask = 0;
static u32 g_rshift = 64;
static void runs_count(const FastTS& f) {
  for (const Run& r : f.runs) {
    u64 h = slot_of(r.person, g_rshift);
    for (;;) {
      u64 k = g_rk[h].load(std::memory_order_relaxed);
      if (k == r.person) break;
      if (k == 0) {
        u64 z = 0;
        if (g_rk[h].compare_exchange_strong(z, r.person, std::memory_order_relaxed)) break;
        if (z == r.person) break;
        continue;
      }
      h = (h + 1) & g_rmask;
    }
    g_rc[h].fetch_add(1, std::memory_order_relaxed);
  }
}
static inline u32 runs_of(u64 p) {
  u64 h = slot_of(p, g_rshift);
  for (;;) {
    u64 k = g_rk[h].load(std::memory_order_relaxed);
    if (k == p) return g_rc[h].load(std::memory_order_relaxed);
    if (k == 0) return 0;
    h = (h + 1) & g_rmask;
  }
}
// Single-run persons are processed in place; the rest go to the general path.
static void runs_process(ThreadState& t, FastTS& f, Scratch& s) {
  const u32 pshift = 64 - g_pbits;
  for (const Run& r : f.runs) {
    const EV* ev = f.rev.data() + r.off;
    if (runs_of(r.person) == 1) {
      u32 m;
      const CK* ck = clicks_of(r.person, m);
      process_core(t, s, r.person, ev, r.n, ck, m);
    } else {
      for (u32 i = 0; i < r.n; i++) t.pb.push(hb(r.person) >> pshift, PRec{r.person, ev[i].ts, ev[i].pc});
    }
  }
}

// ---------------------------------------------------------------- parquet
struct Thrift {
  std::vector<u8> b;
  std::vector<int> stack;
  int last = 0;
  void byte(u8 x) { b.push_back(x); }
  void varint(u64 v) {
    while (v >= 0x80) {
      b.push_back((u8)(v | 0x80));
      v >>= 7;
    }
    b.push_back((u8)v);
  }
  void zz(i64 v) { varint((u64)((v << 1) ^ (v >> 63))); }
  void field(int id, int type) {
    int d = id - last;
    if (d > 0 && d <= 15)
      byte((u8)((d << 4) | type));
    else {
      byte((u8)type);
      zz(id);
    }
    last = id;
  }
  void i32f(int id, i64 v) {
    field(id, 5);
    zz(v);
  }
  void i64f(int id, i64 v) {
    field(id, 6);
    zz(v);
  }
  void strf(int id, const std::string& s) {
    field(id, 8);
    varint(s.size());
    b.insert(b.end(), s.begin(), s.end());
  }
  void begin_struct_field(int id) {
    field(id, 12);
    push();
  }
  void push() {
    stack.push_back(last);
    last = 0;
  }
  void end_struct() {
    byte(0);
    last = stack.back();
    stack.pop_back();
  }
  void list_field(int id, int et, size_t n) {
    field(id, 9);
    list_header(et, n);
  }
  void list_header(int et, size_t n) {
    if (n < 15)
      byte((u8)((n << 4) | et));
    else {
      byte((u8)(0xF0 | et));
      varint(n);
    }
  }
};

enum ColKind { C_STR, C_STR_NULL, C_DATE, C_I64, C_I32 };
struct FastBuf {
  u8* p = nullptr;
  size_t n = 0, cap = 0;
  inline void need(size_t k) {
    if (UNLIKELY(n + k > cap)) {
      size_t nc = (n + k) * 2 + 4096;
      u8* np = (u8*)malloc(nc);
      if (p) {
        memcpy(np, p, n);
        free(p);
      }
      p = np;
      cap = nc;
    }
  }
  inline void put32(u32 v) {
    need(4);
    memcpy(p + n, &v, 4);
    n += 4;
  }
  inline void put64(u64 v) {
    need(8);
    memcpy(p + n, &v, 8);
    n += 8;
  }
  inline void bytes(const void* s, size_t k) {
    need(k);
    memcpy(p + n, s, k);
    n += k;
  }
};
struct Col {
  std::string name;
  ColKind kind;
  std::vector<u8> pre;  // 4-byte length + RLE def levels (nullable only)
  FastBuf vals;         // plain-encoded values
  size_t nrows = 0;
  size_t size() const { return pre.size() + vals.n; }
};
static void put32(std::vector<u8>& b, u32 v) {
  u8 x[4] = {(u8)v, (u8)(v >> 8), (u8)(v >> 16), (u8)(v >> 24)};
  b.insert(b.end(), x, x + 4);
}

struct ColBuilder {
  Col c;
  std::vector<u8> defs;  // one byte per row (0/1)
  void reserve(size_t rows, size_t per) {
    c.vals.need(rows * per);
    if (c.kind == C_STR_NULL) defs.reserve(rows);
  }
  inline void str(const char* p, u32 n) {
    c.vals.need(4 + n);
    memcpy(c.vals.p + c.vals.n, &n, 4);
    memcpy(c.vals.p + c.vals.n + 4, p, n);
    c.vals.n += 4 + n;
    if (c.kind == C_STR_NULL) defs.push_back(1);
    c.nrows++;
  }
  inline void null() {
    defs.push_back(0);
    c.nrows++;
  }
  inline void i32v(i64 v) {
    c.vals.put32((u32)v);
    c.nrows++;
  }
  inline void i64v(i64 v) {
    c.vals.put64((u64)v);
    c.nrows++;
  }
  void finish() {
    if (c.kind != C_STR_NULL) return;
    // RLE/bit-packed hybrid, bit width 1, bit-packed groups of 8.
    std::vector<u8> rle;
    size_t groups = (defs.size() + 7) / 8;
    rle.reserve(groups + 16);
    if (groups) {
      u64 hdr = (groups << 1) | 1;
      while (hdr >= 0x80) {
        rle.push_back((u8)(hdr | 0x80));
        hdr >>= 7;
      }
      rle.push_back((u8)hdr);
      for (size_t g = 0; g < groups; g++) {
        u8 x = 0;
        for (int k = 0; k < 8; k++) {
          size_t i = g * 8 + k;
          if (i < defs.size() && defs[i]) x |= (u8)(1 << k);
        }
        rle.push_back(x);
      }
    }
    put32(c.pre, (u32)rle.size());
    c.pre.insert(c.pre.end(), rle.begin(), rle.end());
  }
};

static void write_all(int fd, const u8* p, size_t n) {
  while (n) {
    ssize_t w = write(fd, p, n);
    if (w <= 0) die("write");
    p += w;
    n -= (size_t)w;
  }
}

static void write_parquet(const std::string& path, std::vector<Col>& cols, size_t nrows) {
  int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) die(("create " + path + ": " + strerror(errno)).c_str());
  static const u8 MAGIC[4] = {'P', 'A', 'R', '1'};
  struct Meta {
    u64 off, size;
  };
  std::vector<Meta> metas;
  std::vector<Thrift> heads(cols.size());
  u64 pos = 4;
  for (size_t ci = 0; ci < cols.size(); ci++) {
    auto& c = cols[ci];
    Thrift& h = heads[ci];
    h.i32f(1, 0);  // DATA_PAGE
    h.i32f(2, (i64)c.size());
    h.i32f(3, (i64)c.size());
    h.begin_struct_field(5);
    h.i32f(1, (i64)nrows);
    h.i32f(2, 0);  // PLAIN
    h.i32f(3, 3);  // RLE
    h.i32f(4, 3);
    h.end_struct();
    h.byte(0);
    metas.push_back({pos, h.b.size() + c.size()});
    pos += h.b.size() + c.size();
  }
  Thrift f;
  f.i32f(1, 1);
  f.list_field(2, 12, cols.size() + 1);
  f.push();
  f.strf(4, "schema");
  f.i32f(5, (i64)cols.size());
  f.end_struct();
  for (auto& c : cols) {
    f.push();
    int type = (c.kind == C_STR || c.kind == C_STR_NULL) ? 6 : ((c.kind == C_DATE || c.kind == C_I32) ? 1 : 2);
    f.i32f(1, type);
    f.i32f(3, c.kind == C_STR_NULL ? 1 : 0);
    f.strf(4, c.name);
    if (c.kind == C_STR || c.kind == C_STR_NULL) {
      f.i32f(6, 0);  // UTF8
      f.begin_struct_field(10);
      f.begin_struct_field(1);  // STRING
      f.end_struct();
      f.end_struct();
    } else if (c.kind == C_DATE) {
      f.i32f(6, 6);  // DATE
      f.begin_struct_field(10);
      f.begin_struct_field(6);  // DATE
      f.end_struct();
      f.end_struct();
    }
    f.end_struct();
  }
  f.i64f(3, (i64)nrows);
  if (nrows) {
    f.list_field(4, 12, 1);
    f.push();
    f.list_field(1, 12, cols.size());
    u64 total = 0;
    for (size_t i = 0; i < cols.size(); i++) {
      auto& c = cols[i];
      int type = (c.kind == C_STR || c.kind == C_STR_NULL) ? 6 : ((c.kind == C_DATE || c.kind == C_I32) ? 1 : 2);
      f.push();
      f.i64f(2, (i64)metas[i].off);
      f.begin_struct_field(3);
      f.i32f(1, type);
      f.list_field(2, 5, 2);
      f.zz(0);
      f.zz(3);
      f.list_field(3, 8, 1);
      f.varint(c.name.size());
      f.b.insert(f.b.end(), c.name.begin(), c.name.end());
      f.i32f(4, 0);
      f.i64f(5, (i64)nrows);
      f.i64f(6, (i64)metas[i].size);
      f.i64f(7, (i64)metas[i].size);
      f.i64f(9, (i64)metas[i].off);
      f.end_struct();
      f.end_struct();
      total += metas[i].size;
    }
    f.i64f(2, (i64)total);
    f.i64f(3, (i64)nrows);
    f.end_struct();
  } else {
    f.list_field(4, 12, 0);
  }
  f.byte(0);
  u32 flen = (u32)f.b.size();
  put32(f.b, flen);
  f.b.insert(f.b.end(), MAGIC, MAGIC + 4);
  std::vector<struct iovec> iov;
  iov.push_back({(void*)MAGIC, 4});
  for (size_t ci = 0; ci < cols.size(); ci++) {
    iov.push_back({heads[ci].b.data(), heads[ci].b.size()});
    if (!cols[ci].pre.empty()) iov.push_back({cols[ci].pre.data(), cols[ci].pre.size()});
    if (cols[ci].vals.n) iov.push_back({cols[ci].vals.p, cols[ci].vals.n});
  }
  iov.push_back({f.b.data(), f.b.size()});
  size_t k = 0;
  while (k < iov.size()) {
    int cnt = (int)std::min<size_t>(iov.size() - k, 512);
    ssize_t w = writev(fd, iov.data() + k, cnt);
    if (w < 0) die("writev");
    // advance over fully written iovecs, trim a partial one
    size_t left = (size_t)w;
    while (k < iov.size() && left >= iov[k].iov_len) {
      left -= iov[k].iov_len;
      k++;
    }
    if (k < iov.size() && left) {
      iov[k].iov_base = (u8*)iov[k].iov_base + left;
      iov[k].iov_len -= left;
    }
  }
  close(fd);
}

static void write_user_daily(const std::string& dir, int part, const std::vector<ThreadState::Row>& rows) {
  ColBuilder uid{{"user_id", C_STR}}, day{{"day", C_DATE}}, ses{{"sessions", C_I32}}, evs{{"events", C_I64}},
      rev{{"revenue_cents", C_I64}}, top{{"top_category", C_STR_NULL}};
  size_t n = rows.size();
  uid.reserve(n, 14);
  day.reserve(n, 4);
  ses.reserve(n, 4);
  evs.reserve(n, 8);
  rev.reserve(n, 8);
  top.reserve(n, 8);
  char buf[40];
  for (auto& r : rows) {
    u32 len;
    const char* s = decode(r.person, buf, len);
    uid.str(s, len);
    day.i32v(g_wsday + r.day);
    ses.i32v(r.sessions);
    evs.i64v((i64)r.events);
    rev.i64v((i64)r.revenue);
    if (r.top < 0)
      top.null();
    else
      top.str(g_cat_names[r.top].data(), (u32)g_cat_names[r.top].size());
  }
  std::vector<Col> cols;
  for (ColBuilder* b : {&uid, &day, &ses, &evs, &rev, &top}) {
    b->finish();
    cols.push_back(std::move(b->c));
  }
  write_parquet(dir + "/part-" + std::to_string(part) + ".parquet", cols, rows.size());
}

static void write_campaign_daily(const std::string& dir, const std::vector<u64>& camp) {
  ColBuilder cid{{"campaign_id", C_STR}}, ch{{"channel", C_STR}}, day{{"day", C_DATE}}, ses{{"sessions", C_I64}},
      pur{{"purchasing_sessions", C_I64}}, rev{{"revenue_cents", C_I64}}, ass{{"assisted_sessions", C_I64}};
  size_t nrows = 0;
  for (u32 c = 0; c <= g_ncamp; c++) {
    for (int d = 0; d < g_ndays; d++) {
      const u64* v = camp.data() + ((size_t)c * g_ndays + d) * 4;
      if (!v[0] && !v[3]) continue;
      const std::string& id = c == g_ncamp ? std::string("organic") : g_camp_ids[c];
      const std::string& chn = c == g_ncamp ? std::string("organic") : g_camp_channels[c];
      cid.str(id.data(), (u32)id.size());
      ch.str(chn.data(), (u32)chn.size());
      day.i32v(g_wsday + d);
      ses.i64v((i64)v[0]);
      pur.i64v((i64)v[1]);
      rev.i64v((i64)v[2]);
      ass.i64v(c == g_ncamp ? 0 : (i64)v[3]);
      nrows++;
    }
  }
  std::vector<Col> cols;
  for (ColBuilder* b : {&cid, &ch, &day, &ses, &pur, &rev, &ass}) {
    b->finish();
    cols.push_back(std::move(b->c));
  }
  write_parquet(dir + "/part-0.parquet", cols, nrows);
}

// ---------------------------------------------------------------- main
#include <chrono>
static double now_s() {
  return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
static double g_t0;
static bool g_verbose = getenv("SS_VERBOSE") != nullptr;
static void mark(const char* what) {
  if (g_verbose) fprintf(stderr, "%8.1f ms  %s\n", (now_s() - g_t0) * 1000, what);
}

struct Barrier {
  std::mutex mu;
  std::atomic<int> count{0};
  std::atomic<int> gen{0};
  int n;
  void wait() {
    int g = gen.load();
    if (count.fetch_add(1) + 1 == n) {
      count.store(0);
      gen.fetch_add(1);
    } else {
      while (gen.load() == g) std::this_thread::yield();
    }
  }
};

int main(int argc, char** argv) {
  g_t0 = now_s();
  const char* dd = getenv("DATA_DIR");
  const char* od = getenv("OUTPUT_DIR");
  std::string data = dd ? dd : "/data", outd = od ? od : "/output";
  if (argc >= 3) {
    data = argv[argc - 2];
    outd = argv[argc - 1];
  }
  const char* ncpu = getenv("NCPU");
  g_nt = ncpu ? atoi(ncpu) : (int)std::thread::hardware_concurrency();
  if (g_nt < 1) g_nt = 1;
  if (g_nt > 64) g_nt = 64;
  init_b36();
  init_years();

  g_intern_map = new std::unordered_map<std::string, u32>();
  g_intern_strs = new std::vector<std::string>();

  std::string man = read_small(data + "/manifest.json");
  g_ws = manifest_value(man, "\"window_start\"");
  g_we = manifest_value(man, "\"window_end\"");
  g_cs = g_ws - WEEK_MS;
  if (g_we - g_cs >= (i64)BAD_TS) die("window too long");
  g_ingest_base_s = g_ws / 1000 - (1LL << 31);
  g_wsday = g_ws / DAY_MS;  // windows start after 1970
  g_ndays = (int)((g_we - 1) / DAY_MS - g_wsday + 1);
  if (g_ndays > 255) die("too many days");
  load_lookups(data);
  mark("lookups");

  std::vector<Chunk> ech, cch, lch;
  size_t ebytes = 0, cbytes = 0, lbytes = 0;
  size_t csz = getenv("SS_CHUNK") ? (size_t)atol(getenv("SS_CHUNK")) : ((size_t)512 << 10);
  make_chunks(list_csv(data + "/events"), 0, csz, ech, ebytes);
  make_chunks(list_csv(data + "/clicks"), 1, csz, cch, cbytes);
  make_chunks(list_csv(data + "/links"), 2, csz, lch, lbytes);
  // Larger event chunks first, then clicks.
  mark("chunks");
  {
    // Scatter blocks need ~0.8x the CSV bytes; reserve 2x + 1 GB, capped by the memory limit.
    size_t want = (ebytes + cbytes + lbytes) * 2 + ((size_t)1 << 30);
    const char* mg = getenv("MEM_GB");
    size_t cap = (size_t)(mg ? atoi(mg) : 24) << 30;
    if (cap > ((size_t)4 << 30)) cap -= (size_t)2 << 30;
    ARENA_RESERVE = want < cap ? want : cap;
    ARENA_RESERVE = (ARENA_RESERVE + SLAB - 1) / SLAB * SLAB;
    g_arena_base = (u8*)mmap(nullptr, ARENA_RESERVE + ((size_t)2 << 20), PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (g_arena_base == MAP_FAILED) die("arena");
    g_arena_base = (u8*)(((uintptr_t)g_arena_base + ((size_t)2 << 20) - 1) & ~(uintptr_t)(((size_t)2 << 20) - 1));
#ifdef MADV_HUGEPAGE
    madvise(g_arena_base, ARENA_RESERVE, MADV_HUGEPAGE);
#endif
  }
  std::vector<Chunk> work = ech;
  work.insert(work.end(), cch.begin(), cch.end());
  std::vector<std::vector<std::pair<u64, u64>>> link_pairs(g_nt);
  std::atomic<size_t> nextl{0};
  std::atomic<size_t> lpairs{0};

  // Fast engine: recover the event-id key from a sample (all threads search).
  KAI = inv40(KA);
  KBI = inv40(KB);
  if (getenv("SS_FAST") && !getenv("SS_GENERAL") && !ech.empty()) {
    key_sample(ech);
    g_nmax = (u64)((double)ebytes / g_avgline * 1.08) + 4096;
    std::vector<std::thread> ks;
    for (int t = 0; t < g_nt; t++) ks.emplace_back(key_search, t, g_nt);
    for (auto& x : ks) x.join();
    std::sort(g_keys_found.begin(), g_keys_found.end());
    g_keys_found.erase(std::unique(g_keys_found.begin(), g_keys_found.end()), g_keys_found.end());
    if (g_keys_found.size() == 1) {
      g_fast = true;
      g_ekey = g_keys_found[0];
      g_psh = 13;
      while ((g_nmax >> g_psh) > 4096) g_psh++;
    }
    if (g_verbose)
      fprintf(stderr, "fast=%d keys=%zu samples=%zu nmax=%llu psh=%u\n", (int)g_fast, g_keys_found.size(),
              g_samp.size(), (unsigned long long)g_nmax, g_psh);
  }
  mark("key");
  // Long ids are about 1 in 8 of distinct ids. Tables only need to be roughly
  // right: when one fills up, further new ids take the exact mutex path.
  {
    size_t uwant = (size_t)(lbytes / 18 * 2 / 8) + (size_t)(ebytes / 4000) + 4096;
    uwant = uwant * (size_t)(getenv("SS_UTAB") ? atoi(getenv("SS_UTAB")) : 2) / 4;
    hex_alloc(0, uwant);
  }
  hex_alloc(1, (size_t)(cbytes / 45 / 8) * 3 / 2 + 4096);
  size_t est_events = ebytes / 70 + 1024;
  g_ebits = 6;
  while ((est_events >> g_ebits) > 16384 && g_ebits < 14) g_ebits++;
  if (getenv("SS_EBITS")) g_ebits = atoi(getenv("SS_EBITS"));
  g_pbits = g_ebits;
  if (getenv("SS_PBITS")) g_pbits = atoi(getenv("SS_PBITS"));
  size_t nb = g_fast ? (size_t)(g_nmax >> g_psh) + 1 : (size_t)1 << g_ebits;
  size_t npb = (size_t)1 << g_pbits;
  if (g_fast) {
    g_fts.resize(g_nt);
    g_ckidx.resize(npb);
  }
  std::atomic<size_t> nextq{0};
  std::atomic<size_t> nruns{0};

  g_ts.resize(g_nt);
  std::atomic<size_t> next{0}, nextb{0}, nextp{0};
  Barrier bar;
  bar.n = g_nt;
  mkdir((outd + "/user_daily").c_str(), 0777);
  mkdir((outd + "/campaign_daily").c_str(), 0777);
  std::vector<std::thread> th;
  for (int t = 0; t < g_nt; t++) {
    th.emplace_back([&, t]() {
      ThreadState& me = g_ts[t];
      me.tid = t;
      if (g_fast) memset(g_fts[t].ckey, 0, sizeof(g_fts[t].ckey));
      me.eb.init(nb, &me.arena);
      me.lb.init(nb, &me.arena);
      me.pb.init(npb, &me.arena);
      me.cb.init(npb, &me.arena);
      me.camp.assign((size_t)(g_ncamp + 1) * g_ndays * 4, 0);
      me.clicks.reserve(cbytes / 40 / g_nt * 5 / 4 + 1024);
      // Links first, all threads: parse, union-find, smallest ID per person.
      for (;;) {
        size_t i = nextl.fetch_add(1);
        if (i >= lch.size()) break;
        parse_links(lch[i], link_pairs[t]);
      }
      lpairs.fetch_add(link_pairs[t].size());
      bar.wait();
      if (t == 0) {
        links_alloc(lpairs.load(), me.arena);
        mark("links parsed");
      }
      bar.wait();
      links_union(link_pairs[t]);
      bar.wait();
      {
        size_t cap = g_lmask + 1, per = (cap + g_nt - 1) / g_nt;
        size_t lo = std::min(cap, per * t), hi = std::min(cap, lo + per);
        links_best(lo, hi);
        bar.wait();
        links_finish(lo, hi);
      }
      bar.wait();
      if (t == 0) mark("links done");
      for (;;) {
        size_t i = next.fetch_add(1);
        if (i >= work.size()) break;
        if (work[i].kind == 0)
          parse_events(work[i], me);
        else
          parse_clicks(work[i], me);
      }
      me.eb.finish();
      me.lb.finish();
      if (g_verbose) fprintf(stderr, "  t%d parse end %.1f\n", t, (now_s() - g_t0) * 1000);
      bar.wait();
      route_clicks(me);
      if (t == 0) mark("parse done");
      if (g_fast) {
        me.cb.finish();
        bar.wait();
        FastTS& f = g_fts[t];
        if (t == 0) odd_resolve();
        // Event ranges and click buckets share one work queue.
        for (;;) {
          size_t i = nextq.fetch_add(1);
          if (i < nb)
            fast_partition((int)i, me, f);
          else if (i < nb + npb)
            build_ck_bucket((int)(i - nb), me);
          else
            break;
        }
        nruns.fetch_add(f.runs.size());
        bar.wait();
        if (t == 0) {
          size_t cap = 1024;
          while (cap < nruns.load() * 2 + 16) cap <<= 1;
          g_rk = (std::atomic<u64>*)me.arena.alloc(cap * 8);
          g_rc = (std::atomic<u32>*)me.arena.alloc(cap * 4);
          memset((void*)g_rk, 0, cap * 8);
          memset((void*)g_rc, 0, cap * 4);
          g_rmask = cap - 1;
          g_rshift = shift_of(cap);
          mark("partitions done");
        }
        bar.wait();
        runs_count(f);
        bar.wait();
        {
          Scratch s;
          s.campcnt.assign(g_ncamp + 1, 0);
          s.since.assign(g_ncamp + 1, 0);
          runs_process(me, f, s);
        }
        me.pb.finish();
        bar.wait();
        if (t == 0) mark("dedup done");
      } else {
       {
        std::vector<DSlot> tk;
        std::vector<const ERec*> tv;
        for (;;) {
          size_t b = nextb.fetch_add(1);
          if (b >= nb) break;
          dedup_bucket((int)b, me, tk, tv);
        }
      }
      me.pb.finish();
      me.cb.finish();
      if (g_verbose) fprintf(stderr, "  t%d dedup end %.1f\n", t, (now_s() - g_t0) * 1000);
      bar.wait();
      if (t == 0) mark("dedup done");
      }
      {
        Scratch s;
        s.campcnt.assign(g_ncamp + 1, 0);
        s.since.assign(g_ncamp + 1, 0);
        for (;;) {
          size_t b = nextp.fetch_add(1);
          if (b >= npb) break;
          person_bucket((int)b, me, s);
        }
      }
      if (g_verbose) fprintf(stderr, "  t%d sess end %.1f\n", t, (now_s() - g_t0) * 1000);
      bar.wait();
      if (t == 0) mark("sessions done");
      std::string ud = outd + "/user_daily";
      write_user_daily(ud, t, me.rows);
    });
  }
  for (auto& x : th) x.join();
  std::vector<u64> camp((size_t)(g_ncamp + 1) * g_ndays * 4, 0);
  for (auto& t : g_ts)
    for (size_t i = 0; i < camp.size(); i++) camp[i] += t.camp[i];
  mark("user_daily written");
#ifdef SS_PROF
  for (int i = 0; i < 4; i++) fprintf(stderr, "pt%d %.1f ms-cpu\n", i, g_pt[i] / 2.8e6);
#endif
  write_campaign_daily(outd + "/campaign_daily", camp);
  mark("end");
  if (g_verbose)
    fprintf(stderr, "hex users %llu/%llu clicks %llu/%llu interned %zu\n", (unsigned long long)g_ht[0].count.load(),
            (unsigned long long)(g_ht[0].mask + 1), (unsigned long long)g_ht[1].count.load(),
            (unsigned long long)(g_ht[1].mask + 1), g_intern_strs->size());
#ifdef SS_PGO_GEN
  __gcov_dump();
#endif
  _exit(0);
}

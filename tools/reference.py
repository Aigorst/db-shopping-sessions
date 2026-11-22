#!/usr/bin/env python3
"""Slow, literal implementation of rules.md, used only to cross-check the fast
binary on generated datasets. Usage: reference.py DATA_DIR OUT_DIR
Writes OUT_DIR/user_daily.csv and OUT_DIR/campaign_daily.csv (compare.py reads CSVs)."""
import csv
import json
import os
import sys
from collections import defaultdict

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
DIGITS = set("0123456789")


def read_lines(path):
    with open(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return [l.decode("utf-8") for l in lines[1:]]  # skip header


def parse_line(line, nf):
    if line == "":
        return None
    parts = line.split(",")
    if len(parts) != nf:
        return None
    out = []
    for p in parts:
        if '"' in p:
            if len(p) >= 2 and p[0] == '"' and p[-1] == '"' and '"' not in p[1:-1]:
                out.append(p[1:-1])
            else:
                return None
        else:
            out.append(p)
    return out


def is_digits(s):
    return s != "" and all(c in DIGITS for c in s)


def civil(s):
    # YYYY-MM-DDTHH:MM:SS -> seconds or None
    if len(s) != 19 or s[4] != "-" or s[7] != "-" or s[10] != "T" or s[13] != ":" or s[16] != ":":
        return None
    parts = [s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19]]
    if not all(is_digits(p) for p in parts):
        return None
    y, mo, d, h, mi, se = map(int, parts)
    if not (1 <= mo <= 12) or d < 1 or h > 23 or mi > 59 or se > 59:
        return None
    leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
    dim = [31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1]
    if d > dim:
        return None
    # days from civil (proleptic Gregorian), valid for year 0..9999
    yy = y - (1 if mo <= 2 else 0)
    era = (yy if yy >= 0 else yy - 399) // 400
    yoe = yy - era * 400
    doy = (153 * (mo + (-3 if mo > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    days = era * 146097 + doe - 719468
    return days * 86400 + h * 3600 + mi * 60 + se


def parse_ts(s):
    if s == "":
        return None
    if is_digits(s):
        v = int(s)
        return v if v < 2**63 else None
    if len(s) == 20 and s[19] == "Z":
        c = civil(s[:19])
        return None if c is None else c * 1000
    if len(s) == 25 and s[19] in "+-" and s[22] == ":" and is_digits(s[20:22]) and is_digits(s[23:25]):
        c = civil(s[:19])
        oh, om = int(s[20:22]), int(s[23:25])
        if c is None or oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60
        return (c - off if s[19] == "+" else c + off) * 1000
    return None


def files(d):
    return sorted(os.path.join(d, f) for f in os.listdir(d) if f.endswith(".csv"))


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws, we = parse_ts(man["window_start"]), parse_ts(man["window_end"])

    products = {}
    for line in read_lines(os.path.join(data, "products.csv")):
        f = parse_line(line, 2)
        if f:
            products[f[0]] = f[1]
    campaigns = {}
    for line in read_lines(os.path.join(data, "campaigns.csv")):
        f = parse_line(line, 2)
        if f and f[0]:
            campaigns[f[0]] = f[1]

    # links: union-find
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for path in files(os.path.join(data, "links")):
        for line in read_lines(path):
            f = parse_line(line, 2)
            if not f or not f[0] or not f[1]:
                continue
            a, b = find(f[0]), find(f[1])
            if a != b:
                parent[a] = b
    best = {}
    for x in list(parent):
        r = find(x)
        if r not in best or x.encode() < best[r].encode():
            best[r] = x
    person = {x: best[find(x)] for x in parent}

    def person_of(u):
        return person.get(u, u)

    # events
    kept = {}
    for path in files(os.path.join(data, "events")):
        for line in read_lines(path):
            f = parse_line(line, 7)
            if not f or not f[0]:
                continue
            ing = parse_ts(f[3])
            if f[0] not in kept or ing < kept[f[0]][0]:
                kept[f[0]] = (ing, f)
    by_person = defaultdict(list)
    for eid, (ing, f) in kept.items():
        _, user, ts_s, _, typ, prod, price_s = f
        ts = parse_ts(ts_s)
        if not user or ts is None or not (ws <= ts < we) or typ not in ("view", "cart", "purchase") or not prod:
            continue
        price = 0
        if typ == "purchase":
            if not is_digits(price_s):
                continue
            price = int(price_s)
            if not (1 <= price <= 10_000_000):
                continue
        if ing - ts > DAY:
            continue
        cat = products.get(prod, "unknown")
        by_person[person_of(user)].append((ts, price, cat))

    clicks = defaultdict(set)
    for path in files(os.path.join(data, "clicks")):
        for line in read_lines(path):
            f = parse_line(line, 4)
            if not f or not f[0]:
                continue
            cid, user, camp, ts_s = f
            ts = parse_ts(ts_s)
            if not user or ts is None or not (ws - WEEK <= ts < we) or camp not in campaigns:
                continue
            clicks[person_of(user)].add((ts, cid, camp))

    user_rows = {}
    camp_rows = defaultdict(lambda: [0, 0, 0, 0])
    for p, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        cl = sorted(clicks.get(p, ()), key=lambda c: (c[0], c[1].encode()))
        sessions = []
        cur = None
        for e in evs:
            if cur is None or e[0] - cur[-1][0] > GAP:
                cur = [e]
                sessions.append(cur)
            else:
                cur.append(e)
        for sess in sessions:
            start = sess[0][0]
            day = start // DAY
            rev = sum(e[1] for e in sess)
            cats = defaultdict(int)
            for e in sess:
                if e[1]:
                    cats[e[2]] += e[1]
            row = user_rows.setdefault((p, day), [0, 0, 0, defaultdict(int)])
            row[0] += 1
            row[1] += len(sess)
            row[2] += rev
            for c, v in cats.items():
                row[3][c] += v
            window = [c for c in cl if start - WEEK <= c[0] <= start]
            if window:
                latest = max(c[0] for c in window)
                credit = min((c for c in window if c[0] == latest), key=lambda c: c[1].encode())[2]
            else:
                credit = "organic"
            r = camp_rows[(credit, day)]
            r[0] += 1
            r[1] += 1 if rev else 0
            r[2] += rev
            for camp in {c[2] for c in window}:
                camp_rows[(camp, day)][3] += 1

    def iso(day):
        import datetime
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=day)).isoformat()

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (p, day), (s, e, r, cats) in user_rows.items():
            top = ""
            if cats:
                top = min(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0]
            w.writerow([p, iso(day), s, e, r, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents",
                    "assisted_sessions"])
        for (c, day), (s, ps, r, a) in camp_rows.items():
            w.writerow([c, "organic" if c == "organic" else campaigns[c], iso(day), s, ps, r, a])


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Compare your Kafka client properties file with the aws-tuned profile. Fully offline.

For every key that the aws-tuned profile sets, the report shows your value (or the Apache Kafka 3.9
client default when you don't set it, read from scripts/profiles/baseline.properties), the aws-tuned
value, a status, and one "what to do" line that links to docs/client-tuning.md.

Statuses:
  OK         same as aws-tuned, or at least as request-efficient
  WARN       differs from aws-tuned; worth measuring, not urgent
  ATTENTION  likely to cost broker request-handler capacity, change durability, or stop the client
  INFO       keys you set that aws-tuned doesn't (informational)

Exit codes: 0 = ran, nothing needs attention; 1 = ran, at least one ATTENTION finding;
2 = usage or input error.

Examples:
  python3 analysis/check_profile.py my-producer.properties
  python3 analysis/check_profile.py my-consumer.properties --role consumer --format markdown
  python3 analysis/check_profile.py scripts/profiles/baseline.properties --format json

Secret-looking values (sasl.jaas.config, *password*, *secret*, *token*, *credential*) are never
printed. ARNs, 12-digit account IDs and Amazon MSK broker host names are masked in every format.
Nothing is sent anywhere: this tool reads local files only.
"""
import argparse
import json
import os
import re
import sys

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_AGAINST = os.path.join(_REPO, "scripts", "profiles", "aws-tuned.properties")
_DEFAULT_DEFAULTS = os.path.join(_REPO, "scripts", "profiles", "baseline.properties")
_MAX_FILE_BYTES = 1024 * 1024

DOC = "docs/client-tuning.md"
DOC_PRODUCER = DOC + "#producer-settings"
DOC_CONSUMER = DOC + "#consumer-settings"
DOC_BOTH = DOC + "#settings-that-apply-to-both"
DOC_BEYOND = DOC + "#beyond-client-settings"
TEST_CASES = "docs/test-cases/README.md"  # index of the blog's three tests
# Amazon MSK quota page (verified 2026-10-27): IAM access control allows 3000 TCP connections per broker
# and a connection creation rate of 100/s per broker (M5 and M7g; 4/s for t3).
MSK_QUOTA_URL = "https://docs.aws.amazon.com/msk/latest/developerguide/limits.html"

# ---------------------------------------------------------------------------------------------------
# Key catalogue: role and the one-line "why" (condensed from docs/client-tuning.md).
# ---------------------------------------------------------------------------------------------------
KEYS = {
    "acks": ("producer", "Durability setting; identical in both shipped profiles so the A/B is fair.", DOC_PRODUCER),
    "linger.ms": ("producer", "Waits to fill a batch: fewer, fuller produce requests per broker.", DOC_PRODUCER),
    "batch.size": ("producer", "Ceiling on bytes per partition batch, so linger.ms can fill larger batches.", DOC_PRODUCER),
    "buffer.memory": ("producer", "Accumulator memory so larger batches don't block send(); size the heap for it.", DOC_PRODUCER),
    "compression.type": ("producer", "Fewer bytes on the wire, in replication and on disk; costs client CPU.", DOC_PRODUCER),
    "send.buffer.bytes": ("producer", "-1 lets the OS auto-tune the socket send buffer.", DOC_PRODUCER),
    "max.in.flight.requests.per.connection": ("producer", "Pipelining per connection; <= 5 keeps idempotence on.", DOC_PRODUCER),
    "enable.idempotence": ("producer", "No duplicates on retry; identical in both shipped profiles.", DOC_PRODUCER),
    "fetch.min.bytes": ("consumer", "Broker holds a fetch until some data exists instead of answering near-empty.", DOC_CONSUMER),
    "fetch.max.wait.ms": ("consumer", "Caps how long the broker holds a fetch; with fetch.min.bytes cuts fetch rate.", DOC_CONSUMER),
    "max.partition.fetch.bytes": ("consumer", "Per-partition cap on a fetch response.", DOC_CONSUMER),
    "max.poll.records": ("consumer", "Fewer, larger polls; keep processing time well under max.poll.interval.ms.", DOC_CONSUMER),
    "max.poll.interval.ms": ("consumer", "Max time between polls before the member leaves the group.", DOC_CONSUMER),
    "session.timeout.ms": ("consumer", "How long the coordinator waits for heartbeats before evicting a member.", DOC_CONSUMER),
    "heartbeat.interval.ms": ("consumer", "Heartbeat cadence; keep it at most 1/3 of session.timeout.ms.", DOC_CONSUMER),
    "partition.assignment.strategy": ("consumer", "Cooperative assignor = incremental rebalances, less fetch/commit churn.", DOC_CONSUMER),
    "receive.buffer.bytes": ("consumer", "-1 lets the OS auto-tune the socket receive buffer for larger fetches.", DOC_CONSUMER),
    "auto.commit.interval.ms": ("consumer", "Committing more often adds OffsetCommit requests to the brokers.", DOC_CONSUMER),
    "connections.max.idle.ms": ("both", "Keeps idle connections open so clients don't reconnect (IAM auth per connection).", DOC_BOTH),
}

# Keys that only one side uses; used to infer the role when --role auto.
PRODUCER_HINTS = {k for k, v in KEYS.items() if v[0] == "producer"} | {
    "key.serializer", "value.serializer", "retries", "delivery.timeout.ms",
    "partitioner.class", "transactional.id", "transaction.timeout.ms", "max.request.size", "max.block.ms",
}
CONSUMER_HINTS = {k for k, v in KEYS.items() if v[0] == "consumer"} | {
    "group.id", "group.instance.id", "group.protocol", "key.deserializer", "value.deserializer",
    "enable.auto.commit", "auto.offset.reset", "isolation.level", "fetch.max.bytes", "client.rack",
}

# The four keys the harness refuses (scripts/lib-msk-env.sh msk_check_tuning_props, k8s/image/entrypoint.sh).
REFUSED_AUTH_KEYS = ("security.protocol", "sasl.mechanism", "sasl.jaas.config", "sasl.client.callback.handler.class")
_SECRET_RE = re.compile(r"password|secret|token|credential", re.I)
_AUTH_PREFIX_RE = re.compile(r"^(sasl\.|ssl\.|security\.protocol$)")

EAGER_ASSIGNORS = {"RangeAssignor", "RoundRobinAssignor", "StickyAssignor"}
COOPERATIVE_ASSIGNORS = {"CooperativeStickyAssignor"}

INT_KEYS = {
    "linger.ms", "batch.size", "buffer.memory", "send.buffer.bytes", "max.in.flight.requests.per.connection",
    "fetch.min.bytes", "fetch.max.wait.ms", "max.partition.fetch.bytes", "max.poll.records",
    "max.poll.interval.ms", "session.timeout.ms", "heartbeat.interval.ms", "receive.buffer.bytes",
    "auto.commit.interval.ms", "connections.max.idle.ms",
}

# Heuristic (blog / this repo), not an AWS-documented threshold.
SMALL_BATCH_BYTES = 32768


# ---------------------------------------------------------------------------------------------------
# Java properties parsing (java.util.Properties.load semantics)
# ---------------------------------------------------------------------------------------------------
_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "f": "\f"}


def _unescape(s):
    out, i = [], 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            n = s[i + 1]
            if n == "u" and re.fullmatch(r"[0-9a-fA-F]{4}", s[i + 2:i + 6] or ""):
                out.append(chr(int(s[i + 2:i + 6], 16)))
                i += 6
                continue
            out.append(_ESCAPES.get(n, n))
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _logical_lines(text):
    """Yield (first_line_number, logical_line) joining backslash continuations; skip comments/blanks."""
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        start = i + 1
        line = lines[i].lstrip(" \t\f")
        i += 1
        if not line or line[0] in "#!":
            continue
        while True:
            trailing = len(line) - len(line.rstrip("\\"))
            if trailing % 2 == 1:  # odd number of backslashes: continuation
                line = line[:-1]
                if i < len(lines):
                    line += lines[i].lstrip(" \t\f")
                    i += 1
                    continue
            break
        yield start, line


def _split_kv(line):
    i = 0
    while i < len(line):
        c = line[i]
        if c == "\\":
            i += 2
            continue
        if c in "=: \t\f":
            break
        i += 1
    key = line[:i]
    rest = line[i:].lstrip(" \t\f")
    if rest[:1] in ("=", ":"):
        rest = rest[1:].lstrip(" \t\f")
    return _unescape(key), _unescape(rest)


def parse_properties(text):
    """Return (dict key->value with last-wins, {key: [line numbers]})."""
    props, seen = {}, {}
    for lineno, line in _logical_lines(text):
        k, v = _split_kv(line)
        if not k:
            continue
        props[k] = v
        seen.setdefault(k, []).append(lineno)
    return props, seen


def load_file(path, what):
    if not os.path.isfile(path):
        raise InputError("%s not found or not a regular file: %s" % (what, path))
    if os.path.getsize(path) > _MAX_FILE_BYTES:
        raise InputError("%s is larger than 1 MiB; is it really a properties file? %s" % (what, path))
    with open(path, "rb") as f:
        raw = f.read()
    if b"\x00" in raw:
        raise InputError("%s looks binary, not a properties file: %s" % (what, path))
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")  # Properties.load(InputStream) uses ISO-8859-1
    return parse_properties(text)


class InputError(Exception):
    pass


# ---------------------------------------------------------------------------------------------------
# Display safety
# ---------------------------------------------------------------------------------------------------
def is_secret_key(key):
    return key == "sasl.jaas.config" or bool(_SECRET_RE.search(key))


def scrub(value):
    v = re.sub(r"arn:aws[a-zA-Z-]*:[^\s,;\"']*", "<arn>", value)
    v = re.sub(r"(?<!\d)\d{12}(?!\d)", "<account-id>", v)
    v = re.sub(r"[A-Za-z0-9.-]+\.kafka(-serverless)?\.[a-z0-9-]+\.amazonaws\.com(\.cn)?", "<msk-host>", v)
    return v


def show(key, value):
    if value is None:
        return None
    if is_secret_key(key):
        return "<redacted>"
    return scrub(value)


# ---------------------------------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------------------------------
def _int(v):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def _norm_acks(v):
    v = (v or "").strip().lower()
    return "all" if v in ("all", "-1") else v


def _acks_rank(v):
    return {"0": 0, "1": 1, "all": 2}.get(_norm_acks(v), None)


def _assignors(v):
    return [x.strip().rsplit(".", 1)[-1] for x in (v or "").split(",") if x.strip()]


def finding(status, key, what, threshold, source, link, yours=None, tuned=None, yours_set=True):
    return {"status": status, "key": key, "yours": yours, "yours_is_default": not yours_set, "aws_tuned": tuned,
            "threshold": threshold, "source": source, "what_to_do": what, "link": link}


def check_key(key, eff, tuned, user, link, raw_user=None):
    """Return (status, what_to_do, threshold, source) for one key. eff = effective user value."""
    src_tuned = "aws-tuned profile"
    heur = "heuristic (blog / this repo)"
    iv, it = _int(eff), _int(tuned)

    if key in INT_KEYS and eff is not None and iv is None:
        return ("ATTENTION", "Not an integer: the Kafka client refuses to start with this value.",
                "integer", "Apache Kafka client config")

    if key == "compression.type":
        c = (eff or "none").strip().lower()
        if c == "zstd":
            return ("OK", "Keep the topic at compression.type=producer (or zstd) so brokers don't recompress.",
                    "zstd", src_tuned)
        if c == "none":
            return ("ATTENTION", "No compression: every byte hits the wire, replication and disk. Try zstd and "
                    "measure client CPU too; keep the topic at compression.type=producer.", "zstd", src_tuned)
        if c in ("snappy", "lz4"):
            return ("WARN", "Consider zstd; measure it with the codec A/B: orchestration/run-ab.sh <cluster-arn> "
                    "aws-tuned aws-tuned-%s." % c, "zstd", heur)
        if c == "gzip":
            return ("WARN", "gzip is usually the most CPU-expensive codec; consider zstd and measure client CPU "
                    "with the codec A/B (orchestration/run-ab.sh).", "zstd", heur)
        return ("WARN", "Unknown codec '%s'; valid: none, gzip, snappy, lz4, zstd." % c, "zstd", "Apache Kafka")

    if key == "linger.ms":
        if iv == 0:
            return ("ATTENTION", "linger.ms=0 sends a batch as soon as the sender is free: many small produce "
                    "requests. Try %s and measure produce latency." % tuned, ">0, aws-tuned %s" % tuned, heur)
        if it is not None and iv < it:
            return ("WARN", "Lower than aws-tuned; batches may stay small at moderate rates. Measure %s." % tuned,
                    ">= %s" % tuned, src_tuned)
        return ("OK", "Request-efficient. Watch produce latency on light traffic.", ">= %s" % tuned, src_tuned)

    if key == "batch.size":
        if iv < SMALL_BATCH_BYTES:
            return ("ATTENTION", "Small batch.size caps records per produce request, so linger.ms can't build "
                    "larger requests. Try %s (and size buffer.memory)." % tuned,
                    ">= %d (heuristic); aws-tuned %s" % (SMALL_BATCH_BYTES, tuned), heur)
        if it is not None and iv < it:
            return ("WARN", "Smaller than aws-tuned; measure %s." % tuned, ">= %s" % tuned, src_tuned)
        return ("OK", "Large enough for linger.ms to fill.", ">= %s" % tuned, src_tuned)

    if key == "buffer.memory":
        if it is not None and iv < it:
            return ("WARN", "If you raise batch.size, raise buffer.memory too, and size heap/pod memory for it.",
                    ">= %s" % tuned, src_tuned)
        return ("OK", "Size the JVM heap and pod memory limit for it.", ">= %s" % tuned, src_tuned)

    if key == "acks":
        ru, rt = _acks_rank(eff), _acks_rank(tuned)
        if ru is None:
            return ("ATTENTION", "Invalid acks '%s'; valid: all, -1, 0, 1." % eff, "all", "Apache Kafka")
        if rt is not None and ru < rt:
            return ("ATTENTION", "Your durability differs from aws-tuned (acks=%s). Durability is a business "
                    "decision, not a tuning knob: decide it with the service owner, and use the same acks in both "
                    "A/B profiles so the comparison is fair. acks!=all also turns idempotence off "
                    "(see enable.idempotence)." % tuned, "same as your production", "this repo (fair A/B)")
        if rt is not None and ru > rt:
            return ("OK", "Your durability is stronger than the --against profile. Keep yours; never weaken "
                    "durability to match a profile.", "never weaker", "this repo")
        return ("OK", "Same durability as aws-tuned.", tuned, src_tuned)

    if key == "enable.idempotence":
        u = (eff or "").strip().lower()
        t = (tuned or "").strip().lower()
        explicit = "enable.idempotence" in (user if raw_user is None else raw_user)
        acks_all = _norm_acks(user.get("acks", "all")) == "all"
        mif = _int(user.get("max.in.flight.requests.per.connection", "5"))
        if u == "true" and explicit and (not acks_all or (mif is not None and mif > 5)):
            return ("ATTENTION", "enable.idempotence=true with acks!=all or max.in.flight > 5: the producer "
                    "fails to start (ConfigException). Fix the conflict; don't drop idempotence to make it start.",
                    "acks=all, max.in.flight <= 5", "Apache Kafka 3.x")
        if u == "true" and not explicit and (not acks_all or (mif is not None and mif > 5)):
            return ("ATTENTION", "Not set, and acks/max.in.flight conflict with idempotence, so Kafka 3.x "
                    "silently turns it off: retries can write duplicates. Durability decision: review with the "
                    "service owner.", "acks=all, max.in.flight <= 5", "Apache Kafka 3.x")
        if u == "false" and t == "true":
            return ("WARN", "Idempotence off: retries can write duplicates. Durability decision; this tool never "
                    "recommends weakening it. Use the same value in both A/B profiles.",
                    "same as your production", "this repo (fair A/B)")
        if u == "true" and t == "false":
            return ("OK", "Stronger than the --against profile. Keep yours.", "never weaker", "this repo")
        return ("OK", "Same as aws-tuned. Needs kafka-cluster:WriteDataIdempotently with IAM.", tuned, src_tuned)

    if key == "max.in.flight.requests.per.connection":
        idem = (user.get("enable.idempotence", "true").strip().lower() == "true")
        if iv > 5 and idem:
            return ("ATTENTION", "> 5 conflicts with idempotence (see enable.idempotence).", "<= 5", "Apache Kafka")
        if iv == 1:
            return ("WARN", "1 serializes produce requests per connection (lower throughput). Keep it only if "
                    "you need ordering without idempotence.", tuned, src_tuned)
        if iv == it:
            return ("OK", "Same as aws-tuned.", tuned, src_tuned)
        return ("WARN", "Differs from aws-tuned; measure.", tuned, src_tuned)

    if key in ("send.buffer.bytes", "receive.buffer.bytes"):
        if iv == it or iv == -1:
            return ("OK", "OS auto-tunes the socket buffer." if iv == -1 else "Same as aws-tuned.", tuned, src_tuned)
        return ("WARN", "Fixed socket buffer; -1 lets the OS auto-tune it (helps larger requests/fetches).",
                tuned, src_tuned)

    if key == "fetch.min.bytes":
        if iv is not None and iv <= 1:
            return ("ATTENTION", "fetch.min.bytes=1 lets the broker answer every fetch immediately, even near "
                    "empty: idle partitions drive many fetch requests. Try %s with fetch.max.wait.ms; watch "
                    "latency on quiet partitions." % tuned, "> 1, aws-tuned %s" % tuned, heur)
        if it is not None and iv < it:
            return ("WARN", "Lower than aws-tuned; measure %s." % tuned, ">= %s" % tuned, src_tuned)
        return ("OK", "Fetches wait for data. Watch latency on quiet partitions.", ">= %s" % tuned, src_tuned)

    if key in ("fetch.max.wait.ms", "max.poll.records"):
        if it is not None and iv < it:
            extra = " Keep processing per poll well under max.poll.interval.ms." if key == "max.poll.records" else ""
            return ("WARN", "Lower than aws-tuned; measure %s.%s" % (tuned, extra), ">= %s" % tuned, src_tuned)
        return ("OK", "At least as request-efficient as aws-tuned.", ">= %s" % tuned, src_tuned)

    if key == "heartbeat.interval.ms":
        st = _int(user.get("session.timeout.ms")) or 45000
        if iv * 3 > st:
            return ("WARN", "heartbeat.interval.ms is above 1/3 of session.timeout.ms (%d); members may be "
                    "evicted on a missed heartbeat." % st, "<= session.timeout.ms / 3", "Apache Kafka docs")

    if key == "partition.assignment.strategy":
        names = _assignors(eff)
        if (user.get("group.protocol", "classic").strip().lower() == "consumer"):
            return ("OK", "group.protocol=consumer: the client ignores partition.assignment.strategy "
                    "(server-side assignment).", "CooperativeSticky", "Apache Kafka")
        eager = [n for n in names if n in EAGER_ASSIGNORS]
        coop = [n for n in names if n in COOPERATIVE_ASSIGNORS]
        migrate = ("Migrating a live group needs two rolling restarts: 1) list CooperativeStickyAssignor first "
                   "plus your current eager assignor; 2) remove the eager one.")
        if coop and not eager:
            return ("OK", "Incremental (cooperative) rebalances.", "CooperativeStickyAssignor only", src_tuned)
        if eager and not coop:
            return ("ATTENTION", "Eager assignor (%s): every rebalance revokes every partition of every member. %s"
                    % (", ".join(eager), migrate), "CooperativeStickyAssignor", src_tuned)
        if eager and coop:
            step = "Do step 2 (remove %s)." % ", ".join(eager) if names[0] in COOPERATIVE_ASSIGNORS else migrate
            return ("WARN", "Eager and cooperative assignors listed: the group still rebalances eagerly until "
                    "the eager one is removed. %s" % step, "CooperativeStickyAssignor only", src_tuned)
        return ("WARN", "Custom assignor (%s); check whether it supports cooperative rebalancing." % eff,
                "CooperativeStickyAssignor", src_tuned)

    if key == "auto.commit.interval.ms":
        if user.get("enable.auto.commit", "true").strip().lower() == "false":
            return ("OK", "enable.auto.commit=false: this key is unused. Check how often your code commits "
                    "(each commit is a request).", ">= 5000", heur)
        if iv < 5000:
            return ("ATTENTION", "Commits more often than every 5 s add OffsetCommit requests on every broker "
                    "that coordinates a group. Use >= 5000.", ">= 5000 (Kafka default)", heur)
        return ("OK", "Committed-offset cadence also adds to the consumer lag you see in CloudWatch.",
                ">= 5000", heur)

    if key == "connections.max.idle.ms":
        if it is not None and iv < it:
            return ("WARN", "Idle connections close sooner, so clients reconnect more. With IAM, each new "
                    "connection is authenticated and Amazon MSK limits the connection creation rate per broker "
                    "(%s)." % MSK_QUOTA_URL, ">= %s" % tuned, "aws-tuned; Amazon MSK quota")
        return ("OK", "Clients keep idle connections instead of reconnecting.", ">= %s" % tuned, src_tuned)

    # Generic: equality with aws-tuned.
    if (eff or "").strip() == (tuned or "").strip():
        return ("OK", "Same as aws-tuned.", tuned, src_tuned)
    return ("WARN", "Differs from aws-tuned; keep it only if it wins when measured.", tuned, src_tuned)


def infer_role(user):
    p = any(k in PRODUCER_HINTS for k in user)
    c = any(k in CONSUMER_HINTS for k in user)
    if p and not c:
        return "producer"
    if c and not p:
        return "consumer"
    return "both"


def evaluate(user, dupes, against, defaults, role):
    findings, extras, auth = [], [], []
    for key, tuned in against.items():
        krole, why, link = KEYS.get(key, ("both", "Set by the --against profile.", DOC))
        if role != "both" and krole not in (role, "both"):
            continue
        is_set = key in user
        eff = user[key] if is_set else defaults.get(key)
        if eff is None:
            f = finding("WARN", key, "Not set and no default known for it (not in the defaults file).",
                        tuned, "aws-tuned profile", link, None, tuned, False)
        else:
            status, what, thr, src = check_key(key, eff, tuned, dict(defaults, **user), link, user)
            f = finding(status, key, what, thr, src, link, eff, tuned, is_set)
        f["why"] = why
        f["role"] = krole
        findings.append(f)
    for key in user:
        if key in against:
            continue
        if key in REFUSED_AUTH_KEYS or _AUTH_PREFIX_RE.match(key):
            refused = key in REFUSED_AUTH_KEYS
            auth.append(finding(
                "ATTENTION", key,
                ("The harness refuses to start a client whose tuning profile sets this key; it adds IAM auth "
                 "itself. Remove it, and never paste it publicly." if refused else
                 "Authentication/TLS key: the harness supplies its own IAM auth; remove it from the profile and "
                 "never paste it publicly."),
                "absent", "scripts/lib-msk-env.sh (msk_check_tuning_props)" if refused else "this repo",
                "README.md#bring-your-own-client-config", user[key], None))
        else:
            extras.append({"status": "INFO", "key": key, "yours": user[key],
                           "what_to_do": "Not set by aws-tuned; keep it identical in both A/B profiles."})
    dup = [finding("WARN", k, "Set %d times (lines %s); the last one wins." % (len(v), ", ".join(map(str, v))),
                   "set once", "java.util.Properties", DOC, None, None)
           for k, v in dupes.items() if len(v) > 1]
    return findings, auth, extras, dup


# ---------------------------------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------------------------------
def _yours_text(f):
    v = show(f["key"], f.get("yours"))
    if v is None:
        return "(unset, no default)"
    return ("default %s" % v) if f.get("yours_is_default") else v


def _md(s):
    return str(s).replace("|", "\\|").replace("\n", " ")


def summarize(groups):
    counts = {"OK": 0, "WARN": 0, "ATTENTION": 0}
    for g in groups:
        for f in g:
            if f["status"] in counts:
                counts[f["status"]] += 1
    return counts


def render(fmt, meta, findings, auth, extras, dup):
    counts = summarize([findings, auth, dup])
    if fmt == "json":
        def clean(f):
            f = dict(f)
            if "yours" in f:
                f["yours"] = show(f["key"], f["yours"])
            if f.get("aws_tuned") is not None:
                f["aws_tuned"] = show(f["key"], f["aws_tuned"])
            return f
        return json.dumps({"file": meta["file"], "against": meta["against"], "role": meta["role"],
                           "summary": counts, "findings": [clean(f) for f in findings],
                           "authentication": [clean(f) for f in auth], "duplicates": dup,
                           "not_in_aws_tuned": [clean(f) for f in extras]}, indent=2)

    out = []
    head = "%s vs %s (role: %s): %d ATTENTION, %d WARN, %d OK" % (
        meta["file"], meta["against"], meta["role"], counts["ATTENTION"], counts["WARN"], counts["OK"])
    if fmt == "markdown":
        out.append("### Client profile check: `%s` vs `%s`\n" % (meta["file"], meta["against"]))
        out.append("Role: %s. **%d ATTENTION, %d WARN, %d OK.** Unset keys show the Apache Kafka 3.9 default.\n"
                   % (meta["role"], counts["ATTENTION"], counts["WARN"], counts["OK"]))
        out.append("| Status | Key | Yours | aws-tuned | Threshold (source) | What to do |")
        out.append("|---|---|---|---|---|---|")
        for f in findings + auth + dup:
            out.append("| %s | `%s` | %s | %s | %s (%s) | %s [doc](%s) |" % (
                f["status"], f["key"], _md(_yours_text(f)) if f.get("yours") is not None or f in findings else "-",
                _md(show(f["key"], f["aws_tuned"]) if f.get("aws_tuned") is not None else "-"),
                _md(f["threshold"]), _md(f["source"]), _md(f["what_to_do"]), f["link"]))
        if extras:
            out.append("\nKeys you set that aws-tuned doesn't (informational):\n")
            for e in extras:
                out.append("- `%s` = `%s`" % (e["key"], _md(show(e["key"], e["yours"]))))
        out.append("\nNext: measure changes with an A/B (`orchestration/run-ab.sh`, `analysis/compare.py`); "
                   "tests: [%s](%s)." % (TEST_CASES, TEST_CASES))
        return "\n".join(out)

    out.append(head)
    out.append("Unset keys show 'default <value>' (Apache Kafka 3.9 client default from %s)." % meta["defaults"])
    sections = [("Keys set by aws-tuned", findings), ("Authentication keys", auth), ("Duplicate keys", dup)]
    for title, items in sections:
        if not items:
            continue
        out.append("")
        out.append("== %s ==" % title)
        for f in items:
            out.append("[%-9s] %s" % (f["status"], f["key"]))
            if f.get("yours") is not None or f in findings:
                tuned = show(f["key"], f["aws_tuned"]) if f.get("aws_tuned") is not None else "-"
                out.append("    yours: %-28s aws-tuned: %s" % (_yours_text(f), tuned))
            if f.get("why"):
                out.append("    why:   %s" % f["why"])
            out.append("    limit: %s  (source: %s)" % (f["threshold"], f["source"]))
            out.append("    do:    %s  -> %s" % (f["what_to_do"], f["link"]))
    if extras:
        out.append("")
        out.append("== Keys you set that aws-tuned doesn't (informational) ==")
        for e in extras:
            out.append("[INFO     ] %s = %s" % (e["key"], show(e["key"], e["yours"])))
    out.append("")
    out.append("Next steps: change one key group at a time and measure it with an A/B at equal MessagesInPerSec")
    out.append("  (orchestration/run-ab.sh <cluster-arn> <profile-a> <profile-b>; analysis/compare.py).")
    out.append("  Tests: %s. Settings guide: %s." % (TEST_CASES, DOC))
    return "\n".join(out)


# ---------------------------------------------------------------------------------------------------
def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("file", help="your Kafka client .properties file")
    p.add_argument("--against", default=_DEFAULT_AGAINST,
                   help="profile to compare with (default: scripts/profiles/aws-tuned.properties)")
    p.add_argument("--defaults", default=_DEFAULT_DEFAULTS,
                   help="file holding Kafka 3.9 client defaults for unset keys (default: the shipped "
                        "scripts/profiles/baseline.properties; if you edited it, point this at a pristine copy)")
    p.add_argument("--role", choices=("producer", "consumer", "both", "auto"), default="auto",
                   help="which side's keys to check (default auto: inferred from the keys in your file)")
    p.add_argument("--format", choices=("text", "markdown", "json"), default="text")
    return p.parse_args(argv)


def main(argv=None):
    a = parse_args(argv)
    try:
        user, dupes = load_file(a.file, "client file")
        against, _ = load_file(a.against, "--against profile")
        defaults, _ = load_file(a.defaults, "--defaults file")
    except (InputError, OSError) as e:
        print("ERROR: %s" % e, file=sys.stderr)
        return 2
    if not against:
        print("ERROR: --against profile has no keys: %s" % a.against, file=sys.stderr)
        return 2
    role = infer_role(user) if a.role == "auto" else a.role
    findings, auth, extras, dup = evaluate(user, dupes, against, defaults, role)
    meta = {"file": os.path.basename(a.file), "against": os.path.basename(a.against),
            "defaults": os.path.relpath(a.defaults, _REPO) if a.defaults.startswith(_REPO) else
            os.path.basename(a.defaults),
            "role": role + (" (inferred)" if a.role == "auto" else "")}
    print(render(a.format, meta, findings, auth, extras, dup))
    return 1 if any(f["status"] == "ATTENTION" for f in findings + auth + dup) else 0


if __name__ == "__main__":
    sys.exit(main())

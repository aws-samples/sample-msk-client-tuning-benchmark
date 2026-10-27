#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""gen-workload.py -- emit Kubernetes Deployments for the Amazon MSK client benchmark.

Microservice model: one consumer group reads one topic, and every consumer
group becomes its own Deployment running MultiGroupConsumer with --groups 1.
By default a group has one member (one pod, one KafkaConsumer). Set
consumers.members_per_group (KafkaConsumer instances per pod) and
consumers.pods_per_group (Deployment replicas) to run several members per
group, for example one consumer per partition. Producers become a small number
of Deployments running MultiProducer with --producers K --topic-mode single and
the total record rate split evenly across pods.

Usage:
  python3 k8s/gen-workload.py k8s/examples/workload-low-density.yaml > /tmp/out.yaml
  python3 k8s/gen-workload.py SPEC [--fanout-mode round-robin|low-volume-topics|explicit]
                                   [--tuning PROFILE] [--image IMAGE]
                                   [--summary-only]

--tuning PROFILE selects the client configuration: pods mount ConfigMap
client-tuning-<PROFILE>. The repo ships `baseline` and `aws-tuned` (default)
in k8s/base/; any other name works once you create its ConfigMap (bring your
own profile, see k8s/README.md). If the spec sets tuning_profiles, only the
listed profiles are accepted.

The manifests go to stdout; the workload summary (pods, groups per topic,
predicted egress multiple for EVERY fan-out mode the spec configures) goes to
stderr and is also embedded as a comment at the top of the output.

Python 3 standard library only. The spec is YAML (a documented subset: block
mappings/lists, flow [lists] / {maps}, plain/quoted scalars, # comments) or JSON.
"""

import argparse
import fnmatch
import json
import os
import re
import sys

# --------------------------------------------------------------------------- errors


class SpecError(Exception):
    pass


def die(msg):
    print("ERROR: " + msg, file=sys.stderr)
    sys.exit(1)


def warn(msg):
    print("WARNING: " + msg, file=sys.stderr)


# --------------------------------------------------------------------------- mini YAML
# Enough YAML for hand-written spec files, with line-numbered errors. Not supported
# (rejected with an error): anchors/aliases, tags, multi-line flow collections,
# block scalars (| and >), multiple documents.

_INT_RE = re.compile(r"^[-+]?[0-9]+$")
_FLOAT_RE = re.compile(r"^[-+]?([0-9]+\.[0-9]*|\.[0-9]+|[0-9]+)([eE][-+]?[0-9]+)?$")


def _strip_comment(s):
    out, quote, i = [], None, 0
    while i < len(s):
        c = s[i]
        if quote:
            out.append(c)
            if quote == '"' and c == "\\" and i + 1 < len(s):
                out.append(s[i + 1])
                i += 2
                continue
            if c == quote:
                quote = None
        else:
            if c in "\"'" and (i == 0 or s[i - 1] in " \t[{,:-"):
                quote = c
            elif c == "#" and (i == 0 or s[i - 1] in " \t"):
                break
            out.append(c)
        i += 1
    return "".join(out).rstrip()


def _split_flow(s, lineno):
    parts, depth, quote, cur, i = [], 0, None, [], 0
    while i < len(s):
        c = s[i]
        if quote:
            cur.append(c)
            if quote == '"' and c == "\\" and i + 1 < len(s):
                cur.append(s[i + 1])
                i += 2
                continue
            if c == quote:
                quote = None
        elif c in "\"'":
            quote = c
            cur.append(c)
        elif c in "[{":
            depth += 1
            cur.append(c)
        elif c in "]}":
            depth -= 1
            cur.append(c)
        elif c == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
        i += 1
    if quote or depth != 0:
        raise SpecError("line %d: unbalanced quotes/brackets in flow collection" % lineno)
    tail = "".join(cur).strip()
    if tail:
        parts.append(tail)
    elif parts:
        raise SpecError("line %d: trailing comma in flow collection" % lineno)
    return parts


def _split_key(s, lineno):
    """Split 'key: rest' at the first ': ' (or trailing ':') outside quotes."""
    quote = None
    for i, c in enumerate(s):
        if quote:
            if c == quote:
                quote = None
        elif c in "\"'" and i == 0:
            quote = c
        elif c == ":" and (i + 1 == len(s) or s[i + 1] in " \t"):
            key = _scalar(s[:i].strip(), lineno)
            if not isinstance(key, str):
                key = str(key)
            return key, s[i + 1:].strip()
    return None, None


def _scalar(s, lineno):
    if s == "":
        return None
    if s[0] == '"':
        if not s.endswith('"') or len(s) < 2:
            raise SpecError("line %d: unterminated double-quoted string: %s" % (lineno, s))
        try:
            return json.loads(s)
        except ValueError as e:
            raise SpecError("line %d: bad double-quoted string %s (%s)" % (lineno, s, e))
    if s[0] == "'":
        if not s.endswith("'") or len(s) < 2:
            raise SpecError("line %d: unterminated single-quoted string: %s" % (lineno, s))
        return s[1:-1].replace("''", "'")
    if s[0] == "[":
        if not s.endswith("]"):
            raise SpecError("line %d: flow list must close on the same line: %s" % (lineno, s))
        return [_scalar(p, lineno) for p in _split_flow(s[1:-1], lineno)]
    if s[0] == "{":
        if not s.endswith("}"):
            raise SpecError("line %d: flow map must close on the same line: %s" % (lineno, s))
        d = {}
        for p in _split_flow(s[1:-1], lineno):
            k, v = _split_key(p, lineno)
            if k is None:
                raise SpecError("line %d: expected 'key: value' in flow map, got %r" % (lineno, p))
            d[k] = _scalar(v, lineno)
        return d
    if s[0] in "&*!|>%@`":
        raise SpecError("line %d: unsupported YAML construct %r (anchors, tags, block scalars)" % (lineno, s))
    low = s.lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    if low in ("null", "~"):
        return None
    if _INT_RE.match(s):
        return int(s)
    if _FLOAT_RE.match(s):
        return float(s)
    return s


def parse_yaml(text):
    lines = []
    for n, raw in enumerate(text.splitlines(), 1):
        if "\t" in raw[: len(raw) - len(raw.lstrip())]:
            raise SpecError("line %d: tab used for indentation" % n)
        stripped = _strip_comment(raw)
        if not stripped.strip():
            continue
        if stripped.strip() in ("---", "..."):
            if lines:
                raise SpecError("line %d: multiple YAML documents are not supported" % n)
            continue
        indent = len(stripped) - len(stripped.lstrip(" "))
        lines.append([indent, stripped.strip(), n])
    if not lines:
        raise SpecError("spec is empty")
    pos = [0]

    def parse_block(indent):
        if lines[pos[0]][1].startswith("- ") or lines[pos[0]][1] == "-":
            return parse_list(indent)
        return parse_map(indent)

    def parse_list(indent):
        out = []
        while pos[0] < len(lines):
            ind, content, n = lines[pos[0]]
            if ind < indent:
                break
            if ind > indent:
                raise SpecError("line %d: unexpected indentation" % n)
            if not (content.startswith("- ") or content == "-"):
                break
            rest = content[1:].strip()
            if rest == "":
                pos[0] += 1
                if pos[0] < len(lines) and lines[pos[0]][0] > indent:
                    out.append(parse_block(lines[pos[0]][0]))
                else:
                    out.append(None)
                continue
            k, _ = _split_key(rest, n) if rest[0] not in "[{\"'" else (None, None)
            if k is not None:
                # "- key: v" opens a mapping whose keys sit at indent+2 (+ extra spaces).
                lines[pos[0]] = [ind + len(content) - len(rest), rest, n]
                out.append(parse_map(lines[pos[0]][0]))
            else:
                out.append(_scalar(rest, n))
                pos[0] += 1
        return out

    def parse_map(indent):
        out = {}
        while pos[0] < len(lines):
            ind, content, n = lines[pos[0]]
            if ind < indent:
                break
            if ind > indent:
                raise SpecError("line %d: unexpected indentation" % n)
            if content.startswith("- "):
                break
            k, v = _split_key(content, n)
            if k is None:
                raise SpecError("line %d: expected 'key: value', got %r" % (n, content))
            if k in out:
                raise SpecError("line %d: duplicate key %r" % (n, k))
            pos[0] += 1
            if v == "":
                if pos[0] < len(lines) and (
                    lines[pos[0]][0] > indent
                    or (lines[pos[0]][0] == indent and lines[pos[0]][1].startswith("- "))
                ):
                    out[k] = parse_block(lines[pos[0]][0])
                else:
                    out[k] = None
            else:
                out[k] = _scalar(v, n)
        return out

    result = parse_block(lines[0][0])
    if pos[0] != len(lines):
        raise SpecError("line %d: could not parse (bad indentation?)" % lines[pos[0]][2])
    return result


def load_spec(path):
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        raise SpecError("cannot read spec %s: %s" % (path, e))
    if path.endswith(".json") or text.lstrip().startswith("{"):
        try:
            return json.loads(text)
        except ValueError as e:
            raise SpecError("invalid JSON in %s: %s" % (path, e))
    return parse_yaml(text)


# --------------------------------------------------------------------------- YAML out


def _q(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    s = str(v)
    # Plain (unquoted) only when it cannot be re-read as anything but this string.
    if _PLAIN_SAFE.match(s) and _scalar(s, 0) == s:
        return s
    return json.dumps(s)  # JSON strings are valid YAML double-quoted scalars


_PLAIN_SAFE = re.compile(r"^[A-Za-z][A-Za-z0-9._/-]*$")


_SIMPLE_KEY = re.compile(r"^[A-Za-z0-9_./-]+$")


def _k(key):
    return key if _SIMPLE_KEY.match(key) and key not in ("true", "false", "null", "yes", "no") else json.dumps(key)


def to_yaml(obj, indent=0):
    pad = " " * indent
    out = []
    if isinstance(obj, dict):
        if not obj:
            return [pad + "{}"]
        for k, v in obj.items():
            if isinstance(v, (dict, list)) and v:
                out.append("%s%s:" % (pad, _k(k)))
                out.extend(to_yaml(v, indent + 2))
            elif isinstance(v, (dict, list)):
                out.append("%s%s: %s" % (pad, _k(k), "{}" if isinstance(v, dict) else "[]"))
            else:
                out.append("%s%s: %s" % (pad, _k(k), _q(v)))
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)) and item:
                sub = to_yaml(item, indent + 2)
                out.append(pad + "- " + sub[0].lstrip())
                out.extend(sub[1:])
            else:
                out.append(pad + "- " + (_q(item) if not isinstance(item, (dict, list)) else ("{}" if isinstance(item, dict) else "[]")))
    else:
        out.append(pad + _q(obj))
    return out


# --------------------------------------------------------------------------- helpers

_RANGE_RE = re.compile(r"^(.*)\{(\d+)\.\.(\d+)\}(.*)$")
_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_LABEL_VALUE = re.compile(r"^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$")
_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


def expand_topics(items, where):
    """'load-bench-{1..12}' -> load-bench-1 .. load-bench-12. Order preserved."""
    if isinstance(items, str):
        items = [items]
    if not isinstance(items, list) or not items:
        raise SpecError("%s must be a non-empty list of topic names" % where)
    out = []
    for it in items:
        if not isinstance(it, str):
            raise SpecError("%s: topic %r is not a string" % (where, it))
        m = _RANGE_RE.match(it)
        if m:
            a, b = int(m.group(2)), int(m.group(3))
            if b < a:
                raise SpecError("%s: bad range in %r" % (where, it))
            out.extend("%s%d%s" % (m.group(1), i, m.group(4)) for i in range(a, b + 1))
        else:
            out.append(it)
    for t in out:
        if not _TOPIC_RE.match(t):
            raise SpecError("%s: invalid Kafka topic name %r" % (where, t))
    if len(set(out)) != len(out):
        raise SpecError("%s: duplicate topics in %s" % (where, out))
    return out


def dns_name(s, where):
    n = re.sub(r"[^a-z0-9-]+", "-", s.lower()).strip("-")
    if len(n) > 63 or not _DNS_LABEL.match(n):
        raise SpecError("%s: %r does not make a valid Kubernetes name (<=63 chars, [a-z0-9-])" % (where, s))
    return n


def label_value(s, where):
    if len(s) > 63 or not _LABEL_VALUE.match(s):
        raise SpecError("%s: %r is not a valid label value (<=63 chars)" % (where, s))
    return s


def get(d, key, default=None, typ=None, where="spec", required=False):
    if not isinstance(d, dict):
        raise SpecError("%s must be a mapping" % where)
    if key not in d or d[key] is None:
        if required:
            raise SpecError("%s.%s is required" % (where, key))
        return default
    v = d[key]
    if typ is not None and not isinstance(v, typ) or (typ in (int, (int, float)) and isinstance(v, bool)):
        raise SpecError("%s.%s has the wrong type (%r)" % (where, key, v))
    return v


def mem_bytes(q, where):
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)(Ki|Mi|Gi|Ti|K|M|G|T)?$", str(q))
    if not m:
        raise SpecError("%s: cannot parse memory quantity %r" % (where, q))
    mult = {None: 1, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12,
            "Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4}[m.group(2)]
    return float(m.group(1)) * mult


def cpu_quantity(q, where):
    s = str(q)
    if not re.match(r"^([0-9]+(\.[0-9]+)?|[0-9]+m)$", s):
        raise SpecError("%s: cannot parse CPU quantity %r" % (where, q))
    return s


_BUF_RE = re.compile(r"^\s*buffer\.memory\s*[=:]\s*([0-9]+)\s*$", re.M)


def base_has_configmap(script_dir, profile):
    """True if a manifest in k8s/base defines ConfigMap client-tuning-<profile>."""
    base = os.path.join(script_dir, "base")
    if not os.path.isdir(base):
        return False
    for fn in sorted(os.listdir(base)):
        if fn.endswith((".yaml", ".yml")):
            with open(os.path.join(base, fn), encoding="utf-8") as f:
                if re.search(r"^\s*name:\s*client-tuning-%s\s*$" % re.escape(profile), f.read(), re.M):
                    return True
    return False


def tuning_buffer_memory(script_dir, profile):
    """(buffer.memory, source) for a profile, or (None, None) if the profile is not in the repo.

    Looks in k8s/base for ConfigMap client-tuning-<profile>, then in
    scripts/profiles/<profile>.properties. Kafka's default (32 MiB) applies when
    the file does not set buffer.memory.
    """
    base = os.path.join(script_dir, "base")
    if os.path.isdir(base):
        for fn in sorted(os.listdir(base)):
            if not fn.endswith((".yaml", ".yml")):
                continue
            with open(os.path.join(base, fn), encoding="utf-8") as f:
                text = f.read()
            if re.search(r"^\s*name:\s*client-tuning-%s\s*$" % re.escape(profile), text, re.M):
                vals = _BUF_RE.findall(text)
                return (int(vals[-1]) if vals else 33554432), "k8s/base/" + fn
    prof = os.path.join(os.path.dirname(script_dir), "scripts", "profiles", profile + ".properties")
    if os.path.isfile(prof):
        with open(prof, encoding="utf-8") as f:
            vals = _BUF_RE.findall(f.read())
        return (int(vals[-1]) if vals else 33554432), "scripts/profiles/%s.properties" % profile
    return None, None


# --------------------------------------------------------------------------- consumers

FANOUT_MODES = ("round-robin", "low-volume-topics", "explicit")


def assign_groups(cons, mode, where="consumers"):
    """Return list of (group_id, topic) for one fan-out mode."""
    total = get(cons, "total_groups", typ=int, where=where, required=True)
    if total <= 0:
        raise SpecError("%s.total_groups must be > 0" % where)
    prefix = get(cons, "group_prefix", default="bench-cg", typ=str, where=where)
    pairs = []
    if mode == "round-robin":
        rr = get(cons, "round_robin", typ=dict, where=where, required=True)
        topics = expand_topics(get(rr, "topics", where=where + ".round_robin", required=True),
                               where + ".round_robin.topics")
        for i in range(total):
            pairs.append(("%s-%d" % (prefix, i), topics[i % len(topics)]))
    elif mode == "low-volume-topics":
        lv = get(cons, "low_volume_topics", typ=dict, where=where, required=True)
        w = where + ".low_volume_topics"
        heavy = expand_topics(get(lv, "heavy_topics", where=w, required=True), w + ".heavy_topics")
        low = expand_topics(get(lv, "low_topics", where=w, required=True), w + ".low_topics")
        if set(heavy) & set(low):
            raise SpecError("%s: heavy_topics and low_topics overlap: %s" % (w, sorted(set(heavy) & set(low))))
        n_heavy = get(lv, "heavy_groups", default=len(heavy), typ=int, where=w)
        if not 0 <= n_heavy <= total:
            raise SpecError("%s.heavy_groups must be between 0 and total_groups (%d)" % (w, total))
        if n_heavy < len(heavy):
            warn("%s.heavy_groups=%d < %d heavy topics: %d topic(s) will have NO consumer group"
                 % (w, n_heavy, len(heavy), len(heavy) - n_heavy))
        hp = get(lv, "heavy_group_prefix", default=prefix, typ=str, where=w)
        lp = get(lv, "low_group_prefix", default=prefix, typ=str, where=w)
        for i in range(total):
            if i < n_heavy:
                pairs.append(("%s-%d" % (hp, i), heavy[i % len(heavy)]))
            else:
                pairs.append(("%s-%d" % (lp, i), low[(i - n_heavy) % len(low)]))
    elif mode == "explicit":
        ex = get(cons, "explicit", typ=list, where=where, required=True)
        i = 0
        for j, ent in enumerate(ex):
            w = "%s.explicit[%d]" % (where, j)
            topic = expand_topics([get(ent, "topic", typ=str, where=w, required=True)], w + ".topic")
            if len(topic) != 1:
                raise SpecError("%s.topic must be a single topic (no {a..b} range)" % w)
            n = get(ent, "groups", typ=int, where=w, required=True)
            p = get(ent, "group_prefix", default=prefix, typ=str, where=w)
            if n <= 0:
                raise SpecError("%s.groups must be > 0" % w)
            for _ in range(n):
                pairs.append(("%s-%d" % (p, i), topic[0]))
                i += 1
        if i != total:
            raise SpecError("%s.explicit assigns %d groups but total_groups is %d" % (where, i, total))
    else:
        raise SpecError("unknown fanout_mode %r (expected one of %s)" % (mode, ", ".join(FANOUT_MODES)))
    ids = [g for g, _ in pairs]
    if len(set(ids)) != len(ids):
        raise SpecError("%s (%s): duplicate consumer group ids" % (where, mode))
    return pairs


def configured_modes(cons):
    modes = []
    if isinstance(cons.get("round_robin"), dict):
        modes.append("round-robin")
    if isinstance(cons.get("low_volume_topics"), dict):
        modes.append("low-volume-topics")
    if isinstance(cons.get("explicit"), list) and cons.get("explicit"):
        modes.append("explicit")
    return modes


# --------------------------------------------------------------------------- producers


def plan_producers(spec, default_record_size):
    sets = get(spec, "producers", default=[], typ=list)
    plans = []
    for j, ps in enumerate(sets):
        w = "producers[%d]" % j
        name = get(ps, "name", typ=str, where=w, required=True)
        topics = expand_topics(get(ps, "topics", where=w, required=True), w + ".topics")
        layout = get(ps, "layout", default="per-topic", typ=str, where=w)
        replicas = get(ps, "replicas", default=1, typ=int, where=w)
        k = get(ps, "producers_per_pod", typ=int, where=w, required=True)
        rsize = get(ps, "record_size", default=default_record_size, typ=int, where=w)
        mode = get(ps, "topic_mode", default="single", typ=str, where=w)
        if replicas <= 0 or k <= 0 or not rsize or rsize <= 0:
            raise SpecError("%s: replicas, producers_per_pod and record_size must be > 0" % w)
        if mode not in ("single", "all"):
            raise SpecError("%s.topic_mode must be single or all" % w)
        mbps = get(ps, "total_mb_per_sec", typ=(int, float), where=w)
        rps = get(ps, "total_records_per_sec", typ=(int, float), where=w)
        if (mbps is None) == (rps is None):
            raise SpecError("%s: set exactly one of total_mb_per_sec / total_records_per_sec" % w)
        if rps is None:
            rps = round(mbps * 1048576 / rsize)  # same formula as 08-start-multi-producers.sh
        if rps <= 0:
            raise SpecError("%s: total rate must be > 0" % w)
        if layout == "per-topic":
            deployments = [(dns_name("%s-%s" % (name, t), w), [t]) for t in topics]
        elif layout == "shared":
            n = get(ps, "deployments", default=1, typ=int, where=w)
            if n <= 0:
                raise SpecError("%s.deployments must be > 0" % w)
            deployments = [(dns_name("%s-%d" % (name, i), w), topics) for i in range(n)]
            if mode == "single" and k % len(topics) != 0:
                warn("%s: producers_per_pod=%d is not a multiple of %d topics; with topic_mode single "
                     "producer i writes topics[i %% %d], so topics get unequal producer counts"
                     % (w, k, len(topics), len(topics)))
        else:
            raise SpecError("%s.layout must be per-topic or shared" % w)
        pods = len(deployments) * replicas
        per_pod = round(rps / pods, 1)
        plans.append({
            "name": name, "where": w, "layout": layout, "replicas": replicas, "k": k,
            "record_size": rsize, "topic_mode": mode, "total_rps": rps, "per_pod_rps": per_pod,
            "deployments": deployments, "resources": get(ps, "resources", typ=dict, where=w),
            "mb_per_sec_uncompressed": rps * rsize / 1048576.0,
            "topics": topics,
        })
    return plans


# --------------------------------------------------------------------------- estimate


def topic_rate(est_rates, topic):
    for pat, v in est_rates.items():
        if fnmatch.fnmatchcase(topic, pat):
            return float(v)
    return None


def fanout_summary(pairs, est_rates, mode):
    per_topic = {}
    for _, t in pairs:
        per_topic[t] = per_topic.get(t, 0) + 1
    rows, egress, missing = [], 0.0, []
    for t, n in per_topic.items():
        r = topic_rate(est_rates, t)
        if r is None:
            missing.append(t)
            continue
        rows.append((t, n, r, n * r))
        egress += n * r
    return {"mode": mode, "per_topic": per_topic, "rows": rows, "egress": egress, "missing": missing}


def compress_rows(rows):
    """Group identical (groups, rate) rows for a readable arithmetic line."""
    buckets = {}
    for t, n, r, _ in rows:
        buckets.setdefault((n, r), []).append(t)
    parts = []
    for (n, r), ts in sorted(buckets.items(), key=lambda x: (-x[0][1], -x[0][0])):
        parts.append((len(ts), n, r, ts))
    return parts


# --------------------------------------------------------------------------- manifests


def resources_block(res, where):
    cpu = cpu_quantity(get(res, "cpu", typ=(str, int, float), where=where, required=True), where)
    mem = str(get(res, "memory", typ=str, where=where, required=True))
    mem_bytes(mem, where)
    block = {"requests": {"cpu": cpu, "memory": mem}, "limits": {"memory": mem}}
    if get(res, "cpu_limit", default=True, typ=bool, where=where):
        block["limits"]["cpu"] = cpu
    return block


def deployment(ctx, name, role, replicas, extra_labels, env, res, java_opts, annotations):
    labels = {
        "app": "msk-bench-client",
        "role": role,
        "app.kubernetes.io/part-of": "msk-bench",
        "msk-bench/workload": ctx["workload"],
    }
    labels.update(extra_labels)
    sel = {"app": "msk-bench-client", "role": role, "msk-bench/deployment": name}
    pod_labels = dict(labels)
    pod_labels["msk-bench/deployment"] = name
    spread_sel = {"app": "msk-bench-client", "role": role, "msk-bench/workload": ctx["workload"]}
    spread = []
    for key in ctx["spread_keys"]:
        spread.append({
            "maxSkew": ctx["max_skew"],
            "topologyKey": key,
            "whenUnsatisfiable": ctx["when_unsatisfiable"],
            "labelSelector": {"matchLabels": spread_sel},
        })
    full_env = [
        {"name": "ROLE", "value": role},
        {"name": "BOOTSTRAP_SERVERS", "valueFrom": {"configMapKeyRef": {
            "name": ctx["bootstrap_cm"], "key": "BOOTSTRAP_SERVERS"}}},
        {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        {"name": "AWS_REGION", "value": ctx["region"]},
        {"name": "REPORT_INTERVAL_MS", "value": str(ctx["report_ms"])},
        {"name": "JAVA_OPTS", "value": java_opts},
    ] + [{"name": k, "value": str(v)} for k, v in env]
    if ctx["extra_props"]:
        full_env.append({"name": "EXTRA_PROPS", "value": ctx["extra_props"]})
    ann = {"msk-bench/tuning": ctx["tuning"]}
    ann.update(annotations)
    pod_spec = {
        "serviceAccountName": ctx["service_account"],
        "terminationGracePeriodSeconds": 60,
        "securityContext": {
            "runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
    }
    if spread:
        pod_spec["topologySpreadConstraints"] = spread
    if ctx["node_selector"]:
        pod_spec["nodeSelector"] = ctx["node_selector"]
    if ctx["tolerations"]:
        pod_spec["tolerations"] = ctx["tolerations"]
    pod_spec["containers"] = [{
        "name": "client",
        "image": ctx["image"],
        "imagePullPolicy": ctx["pull_policy"],
        "env": full_env,
        "resources": res,
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": [
            {"name": "tuning", "mountPath": "/etc/msk-bench/tuning", "readOnly": True},
            {"name": "tmp", "mountPath": "/tmp"},  # nosec B108 - pod emptyDir mount path, not host temp-file use
        ],
    }]
    pod_spec["volumes"] = [
        {"name": "tuning", "configMap": {"name": "client-tuning-" + ctx["tuning"],
                                         "items": [{"key": "client-tuning.properties",
                                                    "path": "client-tuning.properties"}]}},
        {"name": "tmp", "emptyDir": {"sizeLimit": "256Mi"}},
    ]
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "namespace": ctx["namespace"], "labels": labels, "annotations": ann},
        "spec": {
            "replicas": replicas,
            "revisionHistoryLimit": 1,
            # Recreate: a rolling update would briefly run 2 members of a 1-member group
            # (rebalance) or 2x the producer rate.
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": sel},
            "template": {"metadata": {"labels": pod_labels, "annotations": ann}, "spec": pod_spec},
        },
    }


# --------------------------------------------------------------------------- main


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("spec")
    ap.add_argument("--fanout-mode", choices=FANOUT_MODES, help="override consumers.fanout_mode")
    ap.add_argument("--tuning", help="override tuning profile: baseline | aws-tuned | <your profile> "
                                     "(pods mount ConfigMap client-tuning-<profile>)")
    ap.add_argument("--image", help="override container image")
    ap.add_argument("--summary-only", action="store_true", help="print the summary only, no manifests")
    args = ap.parse_args()
    script_dir = os.path.dirname(os.path.abspath(__file__))

    try:
        spec = load_spec(args.spec)
        if not isinstance(spec, dict):
            raise SpecError("top level of the spec must be a mapping")
        workload = label_value(dns_name(get(spec, "name", typ=str, required=True), "name"), "name")
        if args.tuning is not None:
            tuning = args.tuning
            if not tuning.strip():
                raise SpecError("--tuning must name a profile (for example baseline or aws-tuned), got ''")
        else:
            tuning = get(spec, "tuning", default="aws-tuned", typ=str)
            if not tuning.strip():
                raise SpecError("spec key 'tuning' must name a profile (for example baseline or aws-tuned), "
                                "got ''")
        # The profile name becomes part of a ConfigMap name and a label-safe annotation.
        if dns_name(tuning, "tuning") != tuning or len("client-tuning-" + tuning) > 253:
            raise SpecError("tuning profile %r must be lowercase [a-z0-9-] (it names ConfigMap "
                            "client-tuning-<profile>)" % tuning)
        # Optional allowlist: when the spec sets tuning_profiles, only those are accepted.
        allowed_tuning = get(spec, "tuning_profiles", typ=list)
        if allowed_tuning is not None and tuning not in allowed_tuning:
            raise SpecError("tuning %r not in the spec's tuning_profiles %s" % (tuning, allowed_tuning))
        spread = get(spec, "spread", default={}, typ=dict)
        spread_keys = get(spread, "topology_keys", default=["kubernetes.io/hostname"], typ=list, where="spread")
        ctx = {
            "workload": workload,
            "namespace": dns_name(get(spec, "namespace", default="msk-bench", typ=str), "namespace"),
            "image": args.image or get(spec, "image", typ=str, required=True),
            "pull_policy": get(spec, "image_pull_policy", default="IfNotPresent", typ=str),
            "tuning": tuning,
            "service_account": get(spec, "service_account", default="msk-bench-client", typ=str),
            "bootstrap_cm": get(spec, "bootstrap_configmap", default="msk-bootstrap", typ=str),
            "region": get(spec, "aws_region", default="us-east-1", typ=str),
            "report_ms": get(spec, "report_interval_ms", default=60000, typ=int),
            "extra_props": get(spec, "extra_props", default="", typ=str),
            "spread_keys": spread_keys,
            "max_skew": get(spread, "max_skew", default=1, typ=int, where="spread"),
            "when_unsatisfiable": get(spread, "when_unsatisfiable", default="ScheduleAnyway", typ=str,
                                      where="spread"),
            "node_selector": get(spec, "node_selector", default={}, typ=dict),
            "tolerations": get(spec, "tolerations", default=[], typ=list),
        }
        if ctx["when_unsatisfiable"] not in ("ScheduleAnyway", "DoNotSchedule"):
            raise SpecError("spread.when_unsatisfiable must be ScheduleAnyway or DoNotSchedule")
        record_size = get(spec, "record_size", default=625, typ=int)
        res_spec = get(spec, "resources", default={}, typ=dict)
        res_cons = get(res_spec, "consumer", default={"cpu": "250m", "memory": "512Mi"}, typ=dict,
                       where="resources")
        res_prod = get(res_spec, "producer", default={"cpu": "1", "memory": "2Gi"}, typ=dict,
                       where="resources")
        jopts = get(spec, "java_opts", default={}, typ=dict)
        jopts_cons = get(jopts, "consumer", default="-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError",
                         typ=str, where="java_opts")
        jopts_prod = get(jopts, "producer", default="-XX:MaxRAMPercentage=70 -XX:+ExitOnOutOfMemoryError",
                         typ=str, where="java_opts")

        cons = get(spec, "consumers", typ=dict, required=True)
        mode = args.fanout_mode or get(cons, "fanout_mode", default="round-robin", typ=str, where="consumers")
        if mode not in FANOUT_MODES:
            raise SpecError("consumers.fanout_mode %r not one of %s" % (mode, ", ".join(FANOUT_MODES)))
        pairs = assign_groups(cons, mode)
        cons_stagger = get(cons, "stagger_ms", typ=int, where="consumers")
        # Members per group = members_per_group (KafkaConsumer instances in one pod)
        #                   x pods_per_group (Deployment replicas). Default 1 x 1.
        members_per_pod = get(cons, "members_per_group", default=1, typ=int, where="consumers")
        pods_per_group = get(cons, "pods_per_group", default=1, typ=int, where="consumers")
        worker_threads = get(cons, "worker_threads", typ=int, where="consumers")
        latency_sec = get(cons, "latency_report_sec", typ=int, where="consumers")
        if members_per_pod <= 0 or pods_per_group <= 0:
            raise SpecError("consumers.members_per_group and consumers.pods_per_group must be > 0")
        if worker_threads is not None and worker_threads < 0:
            raise SpecError("consumers.worker_threads must be >= 0")
        if latency_sec is not None and latency_sec < 0:
            raise SpecError("consumers.latency_report_sec must be >= 0")

        plans = plan_producers(spec, record_size)

        est = get(spec, "estimate", default={}, typ=dict)
        est_rates = get(est, "topic_mb_per_sec", default={}, typ=dict, where="estimate")
        est_parts = get(est, "partitions_per_topic", default={}, typ=dict, where="estimate")
        # Optional: an egress/ingress ratio you want to reproduce (for example the one
        # measured on your production cluster). When unset, no comparison is printed.
        ref_multiple = get(est, "reference_egress_multiple", typ=(int, float), where="estimate")
        warn_multiple = get(est, "warn_above_multiple", default=2.5, typ=(int, float), where="estimate")
    except SpecError as e:
        die(str(e))

    # ------------------------------------------------------------------ summary
    all_topics = []
    for p in plans:
        all_topics += [t for t in p["topics"] if t not in all_topics]
    for _, t in pairs:
        if t not in all_topics:
            all_topics.append(t)
    ingress = 0.0
    missing_rates = []
    for t in all_topics:
        r = topic_rate(est_rates, t)
        if r is None:
            missing_rates.append(t)
        else:
            ingress += r
    produced = set(t for p in plans for t in p["topics"])
    unproduced = sorted(set(t for _, t in pairs) - produced)

    lines = []
    lines.append("msk-bench workload '%s' (spec %s)" % (workload, os.path.basename(args.spec)))
    lines.append("tuning profile: %s (ConfigMap client-tuning-%s); image: %s" % (tuning, tuning, ctx["image"]))
    lines.append("")
    lines.append("CONSUMERS: %d groups -> %d Deployments x %d replica(s) = %d pods; %d KafkaConsumer "
                 "member(s)/pod -> %d members/group, %d KafkaConsumer clients (fanout_mode: %s)"
                 % (len(pairs), len(pairs), pods_per_group, len(pairs) * pods_per_group, members_per_pod,
                    members_per_pod * pods_per_group, len(pairs) * pods_per_group * members_per_pod, mode))
    lines.append("PRODUCERS:")
    tot_prod_pods = tot_prod_clients = tot_prod_deps = 0
    for p in plans:
        nd = len(p["deployments"])
        pods = nd * p["replicas"]
        tot_prod_deps += nd
        tot_prod_pods += pods
        tot_prod_clients += pods * p["k"]
        lines.append("  %-10s %2d Deployment(s) x %d replica(s) x %d producers/pod = %3d clients; "
                     "%s rec/s total (%.1f MB/s @ %d B uncompressed), %.1f rec/s per pod, layout %s"
                     % (p["name"], nd, p["replicas"], p["k"], pods * p["k"], p["total_rps"],
                        p["mb_per_sec_uncompressed"], p["record_size"], p["per_pod_rps"], p["layout"]))
    lines.append("  total: %d Deployments, %d pods, %d KafkaProducer clients"
                 % (tot_prod_deps, tot_prod_pods, tot_prod_clients))
    lines.append("")
    lines.append("EGRESS ESTIMATE (each group reads ALL of its topic, so egress = sum(groups_on_topic x topic_MB/s))")
    ref_txt = "; reference egress multiple = %.2fx" % ref_multiple if ref_multiple is not None else ""
    lines.append("  ingress = %.2f MB/s (estimate.topic_mb_per_sec over %d topics)%s"
                 % (ingress, len(all_topics) - len(missing_rates), ref_txt))
    if missing_rates:
        lines.append("  NOTE: no estimate.topic_mb_per_sec entry for: %s (excluded)" % ", ".join(missing_rates))
    modes = configured_modes(cons)
    if mode not in modes:
        modes.insert(0, mode)
    selected_summary = None
    for m in modes:
        try:
            pm = pairs if m == mode else assign_groups(cons, m)
        except SpecError as e:
            lines.append("  [%s] not computable: %s" % (m, e))
            continue
        s = fanout_summary(pm, est_rates, m)
        mult = s["egress"] / ingress if ingress else float("nan")
        tag = "SELECTED" if m == mode else "alternative"
        lines.append("  [%s, %s]" % (m, tag))
        terms = []
        for nt, ng, r, ts in compress_rows(s["rows"]):
            shown = ts[0] if nt == 1 else "%s..(%d topics)" % (ts[0], nt)
            lines.append("    %-28s %3d group(s)/topic x %8.4f MB/s x %2d topic(s) = %8.2f MB/s"
                         % (shown, ng, r, nt, ng * r * nt))
            terms.append("%dx%dx%.4g" % (nt, ng, r))
        lines.append(("    egress = %s = %.2f MB/s ; egress/ingress = %.2f / %.2f = %.2fx"
                      % (" + ".join(terms), s["egress"], s["egress"], ingress, mult))
                     + ("  (reference %.2fx)" % ref_multiple if ref_multiple is not None else ""))
        if m == mode:
            selected_summary = (s, mult)
    lines.append("")
    gpt = {}
    for _, t in pairs:
        gpt[t] = gpt.get(t, 0) + 1
    lines.append("GROUPS PER TOPIC (selected): " + ", ".join("%s=%d" % (t, n) for t, n in gpt.items()))

    warnings = []
    if selected_summary and selected_summary[1] == selected_summary[1] and selected_summary[1] > warn_multiple:
        warnings.append(
            "selected fanout_mode '%s' predicts egress %.2fx ingress, above estimate.warn_above_multiple "
            "(%.2fx)%s. Every group on a full-volume topic re-reads that whole topic. "
            "Use 'fanout_mode: low-volume-topics' (or --fanout-mode low-volume-topics) to put most "
            "groups on low-volume topics."
            % (mode, selected_summary[1], warn_multiple,
               "; reference %.2fx" % ref_multiple if ref_multiple is not None else ""))
    members = members_per_pod * pods_per_group
    if members > 1:
        for t in sorted(gpt):
            n_parts = None
            for pat, v in est_parts.items():
                if fnmatch.fnmatchcase(t, pat):
                    n_parts = int(v)
                    break
            if n_parts is not None and members > n_parts:
                warnings.append("topic %s has %d partitions but each group has %d members: %d member(s) "
                                "per group will sit idle" % (t, n_parts, members, members - n_parts))
    if unproduced:
        warnings.append("consumer topics with no producer in this spec: %s" % ", ".join(unproduced))
    buf, buf_src = tuning_buffer_memory(script_dir, tuning)
    if not base_has_configmap(script_dir, tuning):
        prof_rel = "scripts/profiles/%s.properties" % tuning
        src = prof_rel if os.path.isfile(os.path.join(os.path.dirname(script_dir), prof_rel)) \
            else "<your %s.properties>" % tuning
        lines.append("NOTE: k8s/base has no ConfigMap client-tuning-%s; create it in namespace %s before "
                     "applying, or pods stay in ContainerCreating: kubectl -n %s create configmap "
                     "client-tuning-%s --from-file=client-tuning.properties=%s (see k8s/README.md)"
                     % (tuning, ctx["namespace"], ctx["namespace"], tuning, src))
    if "111122223333" in ctx["image"]:
        warnings.append("image %s contains the placeholder account 111122223333; build and push your "
                        "image (k8s/README.md, section 1) and pass --image <uri> or set image: in the spec"
                        % ctx["image"])
    for p in plans:
        res = p["resources"] or res_prod
        try:
            mem = mem_bytes(get(res, "memory", typ=str, where=p["where"] + ".resources", required=True),
                            p["where"])
        except SpecError as e:
            die(str(e))
        pct = re.search(r"MaxRAMPercentage=([0-9.]+)", jopts_prod)
        heap = mem * (float(pct.group(1)) / 100.0 if pct else 0.25)
        if buf and p["k"] * buf * 1.3 > heap:
            warnings.append("%s: %d producers x buffer.memory %d MiB (tuning %s, %s) x 1.3 = %.0f MiB exceeds the "
                            "~%.0f MiB heap of a %s pod; raise memory or lower producers_per_pod"
                            % (p["name"], p["k"], buf // 1048576, tuning, buf_src, p["k"] * buf * 1.3 / 1048576,
                               heap / 1048576, get(res, "memory")))
    for w in warnings:
        lines.append("WARNING: " + w)

    for ln in lines:
        print(ln, file=sys.stderr)
    if args.summary_only:
        return

    # ------------------------------------------------------------------ manifests
    docs = []
    try:
        cres = resources_block(res_cons, "resources.consumer")
        for gid, topic in pairs:
            name = dns_name("cg-" + gid, "consumer group %s" % gid)
            env = [("TOPICS", topic), ("GROUP_PREFIX", gid.rsplit("-", 1)[0]),
                   ("NUM_GROUPS", 1), ("START_INDEX", gid.rsplit("-", 1)[1])]
            if cons_stagger is not None:
                env.append(("STAGGER_MS", cons_stagger))
            if worker_threads is not None:
                env.append(("WORKER_THREADS", worker_threads))
            if members_per_pod > 1:
                env.append(("MEMBERS_PER_GROUP", members_per_pod))
            if latency_sec is not None:
                env.append(("LATENCY_REPORT_SEC", latency_sec))
            docs.append(deployment(
                ctx, name, "consumer", pods_per_group,
                {"group": label_value(gid, "group id"), "topic": label_value(topic, "topic")},
                env, cres, jopts_cons,
                {"msk-bench/consumer-group": gid, "msk-bench/topic": topic, "msk-bench/fanout-mode": mode}))
        for p in plans:
            pres = resources_block(p["resources"] or res_prod, p["where"] + ".resources")
            for dname, topics in p["deployments"]:
                extra = {"group": label_value(p["name"], p["where"] + ".name")}
                if len(topics) == 1:
                    extra["topic"] = label_value(topics[0], "topic")
                env = [("TOPICS", ",".join(topics)), ("NUM_PRODUCERS", p["k"]),
                       ("TOTAL_RECORDS_PER_SEC", p["per_pod_rps"]), ("RECORD_SIZE", p["record_size"]),
                       ("TOPIC_MODE", p["topic_mode"])]
                docs.append(deployment(
                    ctx, dname, "producer", p["replicas"], extra, env, pres, jopts_prod,
                    {"msk-bench/producer-set": p["name"], "msk-bench/topics": ",".join(topics),
                     "msk-bench/records-per-sec-per-pod": str(p["per_pod_rps"])}))
    except SpecError as e:
        die(str(e))
    names = [d["metadata"]["name"] for d in docs]
    dup = sorted(set(n for n in names if names.count(n) > 1))
    if dup:
        die("duplicate Deployment names: %s" % ", ".join(dup))

    out = sys.stdout
    out.write("# GENERATED by k8s/gen-workload.py from %s -- do not edit; regenerate.\n"
              % os.path.basename(args.spec))
    for ln in lines:
        out.write(("# " + ln).rstrip() + "\n")
    for d in docs:
        out.write("---\n")
        out.write("\n".join(to_yaml(d)) + "\n")


if __name__ == "__main__":
    main()

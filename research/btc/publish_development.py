"""Verify, package and optionally publish a 1–31 day batch of BTC development data.

Example: python publish_development.py DATA/BTC OUT 2026-01-07 2026-01-09 --publish
One deterministic tar per UTC day plus a batch checksum index. Final data is sealed.
"""

import argparse
import gzip
import hashlib
import io
import json
import math
import re
import subprocess
import tarfile
import tempfile
import time as clock
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

REPO = "TR4-io/hyperliquidamm"
HOUR = 3600000
FIRST_ANNUAL = date(2026, 1, 6)
VALIDATION = date(2026, 8, 27)
FINAL = date(2026, 10, 26)
FINAL_MS = int(datetime.combine(FINAL, time(), tzinfo=UTC).timestamp() * 1000)
CONTEXT_FIELDS = {
    "funding",
    "open_interest",
    "prev_day_px",
    "day_ntl_vlm",
    "premium",
    "oracle_px",
    "mark_px",
    "mid_px",
    "impact_bid_px",
    "impact_ask_px",
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def days(start, end):
    require(
        FIRST_ANNUAL <= start <= end < FINAL and (end - start).days < 31,
        "Use 1–31 completed annual development days beginning January 6",
    )
    require(
        (start < VALIDATION) == (end < VALIDATION), "Batch would cross a study split"
    )
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def finite(value):
    return (
        type(value) in (int, float, str)
        and math.isfinite(float(value))
        and (not isinstance(value, str) or re.fullmatch(r"[-+0-9.Ee]+", value))
    )


def count(value):
    return type(value) is int and value >= 0


def eligible_time(value, hour_start):
    # Venue partition times can straddle an hour. Never export a final-period observation.
    return count(value) and hour_start - HOUR < value < min(
        hour_start + 2 * HOUR, FINAL_MS
    )


def verify_clock(clock, extra):
    fields = {"first", "last", "rows", "max_gap_ms"}
    require(set(clock) in (fields, fields | extra), "Unexpected clock fields")
    require(
        all(count(clock[key]) for key in fields) and clock["first"] <= clock["last"],
        "Invalid clock values",
    )
    for pair in clock.get("gaps_gt_10s", []):
        require(
            len(pair) == 2 and all(count(t) for t in pair) and pair[0] < pair[1],
            "Invalid gap",
        )


def verify_pair(packet, manifest, stamp, hour, context=False):
    meta = json.loads(manifest)
    start = int(
        datetime.combine(date.fromisoformat(stamp), time(), tzinfo=UTC).timestamp()
        * 1000
    )
    t = start + hour * HOUR
    common = {"hour", "sources", "origin", "packet_bytes", "packet_sha256"}
    expected = common | (
        {"rows"}
        if context
        else {
            "books",
            "venue_blocks",
            "unique_takers",
            "crossed_rows",
            "btc_account_rows",
            "stale_books_skipped",
        }
    )
    require(set(meta) == expected and meta["hour"] == t, "Unexpected packet manifest")
    require(
        meta["packet_bytes"] == len(packet) and meta["packet_sha256"] == digest(packet),
        "Packet does not match its source manifest",
    )
    official = (
        [("hyperliquid-archive", f"asset_ctxs/{stamp}.csv.lz4")]
        if context
        else [
            ("hyperliquid-archive", f"market_data/{stamp}/{hour}/l2Book/BTC.lz4"),
            ("hl-mainnet-node-data", f"node_fills_by_block/hourly/{stamp}/{hour}.lz4"),
        ]
    )
    require(len(meta["sources"]) == len(official), "Unexpected source count")
    for source, (bucket, key) in zip(meta["sources"], official):
        require(
            set(source) == {"bucket", "key", "bytes", "sha256"}
            and source["bucket"] == bucket
            and source["key"] == key
            and count(source["bytes"])
            and source["bytes"] > 0
            and re.fullmatch("[0-9a-f]{64}", source["sha256"]),
            "Unexpected source object",
        )
    etags = meta["origin"].get("etags", [])
    require(
        set(meta["origin"]) == {"etags"}
        and len(etags) == len(official)
        and all(re.fullmatch(r'"[0-9a-f]{32}(-[0-9]+)?"', x) for x in etags),
        "Invalid source ETag",
    )
    obj = json.loads(gzip.decompress(packet))
    if context:
        require(meta["rows"] == len(obj) == 24, "Incomplete hourly BTC context")
        for i, row in enumerate(obj):
            require(
                set(row) == CONTEXT_FIELDS | {"coin", "time"}
                and row["coin"] == "BTC"
                and row["time"] == t + i * HOUR
                and all(finite(row[k]) for k in CONTEXT_FIELDS),
                "Invalid BTC context sample",
            )
        return
    verify_clock(meta["books"], set())
    verify_clock(meta["venue_blocks"], {"gaps_gt_10s"})
    require(
        set(obj) == {"books", "trades"}
        and len(obj["books"]) == meta["books"]["rows"]
        and len(obj["trades"]) == meta["unique_takers"]
        and all(
            count(meta[k])
            for k in ("crossed_rows", "btc_account_rows", "stale_books_skipped")
        ),
        "Invalid market packet",
    )
    for row in obj["books"]:
        require(
            set(row) == {"time", "levels"}
            and eligible_time(row["time"], t)
            and len(row["levels"]) == 2,
            "Invalid book",
        )
        for side in row["levels"]:
            require(
                side
                and all(
                    set(level) in ({"px", "sz"}, {"px", "sz", "n"})
                    and all(
                        finite(level[k]) and float(level[k]) > 0 for k in ("px", "sz")
                    )
                    and count(level.get("n", 0))
                    for level in side
                ),
                "Invalid book level",
            )
    tids = set()
    for tr in obj["trades"]:
        require(
            set(tr) == {"tid", "px", "sz", "side", "time"}
            and eligible_time(tr["time"], t)
            and tr["side"] in ("A", "B")
            and re.fullmatch(r"[0-9]+", str(tr["tid"]))
            and all(finite(tr[k]) and float(tr[k]) > 0 for k in ("px", "sz")),
            "Invalid BTC trade",
        )
        tids.add(str(tr["tid"]))
    require(len(tids) == len(obj["trades"]), "Duplicate trade ID")


def pack_day(root, out, day):
    stamp = day.strftime("%Y%m%d")
    split = "training" if day < VALIDATION else "validation"
    relative = Path("data/btc/annual", split, stamp)
    folder = root / "annual" / split / stamp
    start = int(datetime.combine(day, time(), tzinfo=UTC).timestamp() * 1000)
    paths = [
        (f"{start + i * HOUR}.json.gz", f"{start + i * HOUR}.manifest.json", i)
        for i in range(24)
    ]
    paths.append(("context-samples.json.gz", "context.manifest.json", 0))
    asset = out / f"btc-{split}-{stamp}.tar"
    sums = []
    with tempfile.NamedTemporaryFile(dir=out) as temp:
        with tarfile.open(fileobj=temp, mode="w", format=tarfile.USTAR_FORMAT) as tar:
            for packet_name, manifest_name, hour in paths:
                packet = (folder / packet_name).read_bytes()
                manifest = (folder / manifest_name).read_bytes()
                verify_pair(
                    packet,
                    manifest,
                    stamp,
                    hour,
                    packet_name == "context-samples.json.gz",
                )
                for name, data in [(packet_name, packet), (manifest_name, manifest)]:
                    path = str(relative / name)
                    member = tarfile.TarInfo(path)
                    member.size = len(data)
                    tar.addfile(member, io.BytesIO(data))
                    sums.append(f"{digest(data)}  {path}\n")
            data = "".join(sums).encode()
            member = tarfile.TarInfo(str(relative / "SHA256SUMS"))
            member.size = len(data)
            tar.addfile(member, io.BytesIO(data))
        temp.flush()
        fingerprint = file_digest(Path(temp.name))
        require(
            Path(temp.name).stat().st_size < 2_000_000_000,
            "Daily asset exceeds GitHub size limit",
        )
        try:
            asset.hardlink_to(temp.name)
        except FileExistsError:
            require(
                file_digest(asset) == fingerprint,
                "Published inputs have changed: " + asset.name,
            )
    return asset, fingerprint


def api(endpoint, body=None, paginate=False):
    args = ["gh", "api", f"repos/{REPO}/{endpoint}"]
    if body is not None:
        args += ["--input", "-"]
    if paginate:
        args += ["--paginate", "--jq", ".[] | @json"]
    for attempt in range(3):
        response = subprocess.run(
            args,
            input=json.dumps(body).encode() if body is not None else None,
            check=False,
            capture_output=True,
        )
        if response.returncode == 0:
            raw = response.stdout
            break
        if body is not None or attempt == 2:
            raise RuntimeError(
                "GitHub release API failed: "
                + response.stderr.decode(errors="replace").strip()
            )
        clock.sleep(2**attempt)
    return (
        [json.loads(line) for line in raw.splitlines()] if paginate else json.loads(raw)
    )


def publish(tag, sums, out, split, dates):
    existing = [
        r for r in api("releases?per_page=100", paginate=True) if r["tag_name"] == tag
    ]
    title = f"BTC {split} packets, {dates[0]} through {dates[-1]} UTC"
    checksum = out / "SHA256SUMS"
    notes = (
        f"Each tar contains 24 verified hourly BTC book and taker-trade packets, their official "
        f"source manifests, and 24 sampled hourly context rows. Each day has its own checksum list. "
        f"Download the assets together and run `sha256sum -c SHA256SUMS` before extracting. "
        f"Sampled books can contain gaps. Batch SHA256SUMS digest: `{file_digest(checksum)}`."
    )
    if existing:
        require(
            len(existing) == 1
            and existing[0]["name"] == title
            and existing[0]["body"] == notes,
            "Release tag conflicts with another publication",
        )
        release = existing[0]
    else:
        release = api(
            "releases",
            {
                "tag_name": tag,
                "target_commitish": "btc-development-data",
                "name": title,
                "body": notes,
                "draft": True,
                "make_latest": "false",
            },
        )
    endpoint = f"releases/{release['id']}/assets?per_page=100"
    desired = {**sums, checksum.name: file_digest(checksum)}

    def listed():
        result = {r["name"]: r for r in api(endpoint, paginate=True)}
        require(result.keys() <= desired.keys(), "Unexpected remote asset")
        return result

    remote = listed()
    for name, expected in desired.items():
        if name not in remote:
            require(release["draft"], "Published release is missing expected assets")
            subprocess.run(
                ["gh", "release", "upload", tag, str(out / name), "--repo", REPO],
                check=True,
            )
            remote = listed()
        asset = remote[name]
        require(
            asset["state"] == "uploaded"
            and asset["size"] == (out / name).stat().st_size,
            "Incomplete remote asset: " + name,
        )
        if asset.get("digest"):
            require(
                asset["digest"] == "sha256:" + expected,
                "Remote digest mismatch: " + name,
            )
        else:
            with tempfile.TemporaryDirectory(dir=out) as tmp:
                subprocess.run(
                    [
                        "gh",
                        "release",
                        "download",
                        tag,
                        "--repo",
                        REPO,
                        "--pattern",
                        name,
                        "--dir",
                        tmp,
                    ],
                    check=True,
                )
                require(
                    file_digest(Path(tmp) / name) == expected,
                    "Downloaded asset digest mismatch: " + name,
                )
    require(set(listed()) == set(desired), "Incomplete release")
    if release["draft"]:
        subprocess.run(
            [
                "gh",
                "release",
                "edit",
                tag,
                "--repo",
                REPO,
                "--draft=false",
                "--latest=false",
            ],
            check=True,
        )
    done = api(f"releases/tags/{tag}")
    require(
        not done["draft"] and len(done["assets"]) == len(desired),
        "Release failed remote verification",
    )
    print(done["html_url"])


def publish_git_batch(tag, out, sums):
    """Single-branch shallow fetches keep each fallback checkout bounded to one batch."""
    url = f"https://github.com/{REPO}.git"
    branch = "data-" + tag
    ref = "refs/heads/" + branch
    with tempfile.TemporaryDirectory(dir=out) as temp:
        checkout = Path(temp)

        def git(*args):
            return subprocess.check_output(["git", "-C", str(checkout), *args])

        git("init", "-q", "-b", branch)
        git("remote", "add", "origin", url)
        index = []
        for name in sorted(sums):
            require(file_digest(out / name) == sums[name], "Changed local asset: " + name)
            with tarfile.open(out / name) as tar:
                tar.extractall(checkout, filter="data")
        for p in sorted((checkout / "data").rglob("*")):
            if p.is_file():
                index.append(f"{file_digest(p)}  {p.relative_to(checkout)}\n")
        (checkout / "SHA256SUMS").write_text("".join(index))
        (checkout / "README.md").write_text(
            f"# Hyperliquid BTC development data\n\nBatch `{tag}` contains verified hourly BTC book "
            "and taker-trade packets, official source manifests and sampled BTC asset context. "
            "Individual sampled books can have data gaps. Run `sha256sum -c SHA256SUMS` in this checkout "
            "to verify the data files. The date range in the branch name is inclusive in UTC.\n"
        )
        git("add", "--", "data", "SHA256SUMS", "README.md")
        tree = git("write-tree").strip()
        lookup = subprocess.run(
            ["git", "ls-remote", "--exit-code", url, ref],
            check=False,
            capture_output=True,
        )
        if lookup.returncode == 2:
            git(
                "-c",
                "user.name=Codex",
                "-c",
                "user.email=codex@users.noreply.github.com",
                "commit",
                "-q",
                "-m",
                "Publish verified " + tag,
            )
            # Require a new branch; a competing publisher cannot be overwritten.
            git("push", "origin", "HEAD:" + ref, "--force-with-lease=" + ref + ":")
        elif lookup.returncode:
            raise RuntimeError(lookup.stderr.decode(errors="replace"))
        git("fetch", "--quiet", "--depth=1", "origin", ref)
        require(
            git("rev-parse", "FETCH_HEAD^{tree}").strip() == tree,
            "Different remote batch tree",
        )
        checksum_rows = git("show", "FETCH_HEAD:SHA256SUMS").decode().splitlines()
        for line in checksum_rows:
            expected, name = line.split("  ", 1)
            require(
                digest(git("show", "FETCH_HEAD:" + name)) == expected,
                "Remote Git checksum mismatch",
            )
        commit = git("rev-parse", "FETCH_HEAD").decode().strip()
        print(
            f"https://github.com/{REPO}/tree/{branch} {commit} ({len(checksum_rows)} file hashes)"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_root", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("start", type=date.fromisoformat)
    parser.add_argument("end", type=date.fromisoformat)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--publish", action="store_true", help="publish and verify a GitHub Release"
    )
    mode.add_argument(
        "--publish-git-batch",
        action="store_true",
        help="use a date-bounded data-only branch when Releases is unavailable",
    )
    args = parser.parse_args()
    dates = days(args.start, args.end)
    split = "training" if args.start < VALIDATION else "validation"
    tag = f"btc-{split}-{args.start:%Y%m%d}-{args.end:%Y%m%d}"
    out = args.output / tag
    out.mkdir(parents=True, exist_ok=True)
    sums = {}
    for day in dates:
        p, s = pack_day(args.data_root, out, day)
        sums[p.name] = s
        print(day, s, p.name, flush=True)
    index = "".join(f"{s}  {name}\n" for name, s in sums.items()).encode()
    checksum = out / "SHA256SUMS"
    with tempfile.NamedTemporaryFile(dir=out) as f:
        f.write(index)
        f.flush()
        try:
            checksum.hardlink_to(f.name)
        except FileExistsError:
            require(checksum.read_bytes() == index, "Changed batch inputs")
    if args.publish:
        publish(tag, sums, out, split, dates)
    elif args.publish_git_batch:
        publish_git_batch(tag, out, sums)


if __name__ == "__main__":
    main()

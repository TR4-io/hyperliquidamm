"""Extract both original here-documents as bytes without executing the Install script."""

import hashlib
import re
import sys
from pathlib import Path

TARGETS = {
    b"BTC_QUOTE_RESEARCH": (
        "btc-quote-model.cjs",
        "72e98c7dd87de8814719f07b95b3b66b2a7aa622fc1e11f70fd5e6e54b3dd606",
    ),
    b"BTC_ACCOUNT_REPLAY": (
        "btc-account-replay.cjs",
        "9c9e60ebf0a5eefc91ea5df7dbf6fb7d667fd53845d3becb00f3471c71a1c7ab",
    ),
}


def recover(source, dest):
    lines = source.read_bytes().splitlines(keepends=True)
    result = {}
    for marker, (name, wanted) in TARGETS.items():
        pattern = rb"<<\s*['\"]?" + marker + rb"['\"]?(?=\s|$)"
        starts = [i + 1 for i, line in enumerate(lines) if re.search(pattern, line)]
        if len(starts) != 1:
            raise ValueError("Expected one heredoc for " + name)
        a = starts[0]
        b = next(i for i in range(a, len(lines)) if lines[i].rstrip(b"\r\n") == marker)
        data = b"".join(lines[a:b])
        if hashlib.sha256(data).hexdigest() != wanted:
            raise ValueError("SHA-256 mismatch for " + name)
        result[name] = data
    dest.mkdir(parents=True, exist_ok=False)
    for name, data in result.items():
        (dest / name).write_bytes(data)
        print(name, hashlib.sha256(data).hexdigest())


if __name__ == "__main__":
    recover(Path(sys.argv[1]), Path(sys.argv[2]))

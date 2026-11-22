"""Faster deterministic gzip for bulk acquisition, preserving the full packet JSON."""

from functools import partial

from isal import igzip

from archive import normalize as normalize_hour
from archive import write_packet

# Level 3 is modestly larger than zlib level 9 but much faster on sampled L2.
_write_packet = partial(
    write_packet, gzip_writer=partial(igzip.IGzipFile, compresslevel=3)
)


def normalize(t, paths, out, origin):
    return normalize_hour(t, paths, out, origin, packet_writer=_write_packet)

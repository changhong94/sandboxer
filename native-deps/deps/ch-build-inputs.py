#!/usr/bin/env python3
"""Fingerprint CH source, patch series, builder and effective compiler inputs."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys


def fingerprint(source, patches):
    digest = hashlib.sha256()

    def add(label, data):
        digest.update(label.encode() + b"\0" + str(len(data)).encode() + b"\0" + data)

    def git(*args):
        return subprocess.check_output(["git", "-C", str(source), *args])

    add("head", git("rev-parse", "HEAD"))
    add("tracked", git("diff", "--no-ext-diff", "--binary", "HEAD"))
    for raw in sorted(git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")):
        if raw:
            name = os.fsdecode(raw)
            add("untracked:" + name, (source / name).read_bytes())
    for patch in sorted(patches.glob("*.patch")):
        add("patch:" + patch.name, patch.read_bytes())
    for name in ("build-cloud-hypervisor.sh", "common.sh", "ch-build-inputs.py"):
        add("builder:" + name, Path(__file__).with_name(name).read_bytes())
    add("rustc", subprocess.check_output(["rustc", "-vV"]))
    add("cargo", subprocess.check_output(["cargo", "-V"]))
    for name, value in sorted(os.environ.items()):
        if name.startswith(("RUST", "CARGO_TARGET_", "CARGO_PROFILE_", "CC_", "CFLAGS_")) or name in ("CC", "CFLAGS", "CROSS_PREFIX", "CARGO_HOME"):
            add("env:" + name, value.encode())
    return digest.hexdigest()


if __name__ == "__main__":
    print(fingerprint(Path(sys.argv[1]), Path(sys.argv[2])))

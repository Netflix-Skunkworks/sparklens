#!/bin/bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
WORK_DIR=$(mktemp -d)
# deletes the temp directory
function cleanup {
  #rm -rf "$WORK_DIR"
  echo "Deleted temp working directory $WORK_DIR"
}

# register the cleanup function to be called on the EXIT signal
trap cleanup EXIT

set -ex
#git fetch origin
#git fetch oss
ORIGINAL=$(pwd)
cp -af ./ "${WORK_DIR}"
cd "${WORK_DIR}"
git branch -d backup || echo "no backup branch"
git push -d oss backup || echo "no remote backup branch to delete"
(git checkout oss/main && git checkout -b backup && git push --force oss backup) || echo "Nothing to backup."
git checkout main
git branch -d prepare-export || echo "ok cool no prepare export branch"
git checkout -b prepare-export
git filter-repo --invert-paths  --path-regex "gradle*" --path ".netflix" --path "dependencies.lock" --path src/test/scala/com/qubole/sparklens/app/SparklensAppSuite.scala --path src/test/scala/com/qubole/sparklens/app/NetflixSparklensAppSuite.scala --path src/test/replayer-files/replayer-console-output.log --path .newt.yml  --force
# Note expressions doesn't seem to be working so we try two different things
# git filter-repo --replace-text ${SCRIPT_DIR}/expressions.txt
git filter-repo --blob-callback '
# Skip binary files
if not b"\0" in blob.data[0:8192]:
  import re
  orig = blob.data
  rewrite = re.sub("\"3.3.100\"", "\"3.3.4\"", blob.data.decode()).encode()
  rewrite = re.sub("", "", rewrite.decode()).encode()
  rewrite = re.sub("", "", rewrite.decode()).encode()
  rewrite = re.sub("# NETFLIX-BUILD\s*\n.*", "", rewrite.decode(), flags=re.S).encode()
  blob.data = rewrite' --force
cp ${ORIGINAL}/.git/config ./.git/config
git push origin prepare-export --force-with-lease || echo "Failed to push prepare export to internal"
echo "Enabling workflows before external push"
git filter-repo --path-rename .github/workflows-not-enabled:.github/workflows
git push oss prepare-export --force-with-lease || echo "No OSS remote setup yet or upstream changed"

#!/bin/sh

find ./consensus/eirene -type f -exec sed -i '' 's/bor/eirene/g; s/Bor/Eirene/g; s/heimdall/tendermint/g; s/Heimdall/Tendermint/g' {} \;
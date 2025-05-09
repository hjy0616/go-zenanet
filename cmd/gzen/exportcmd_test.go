// Copyright 2022 The go-zenanet Authors
// This file is part of go-zenanet.
//
// go-zenanet is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-zenanet is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-zenanet. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/zenanetwork/go-zenanet/common"
)

// TestExport does a basic test of "gzen export", exporting the test-genesis.
func TestExport(t *testing.T) {
	t.Parallel()
	outfile := fmt.Sprintf("%v/testExport.out", t.TempDir())
	gzen := runGzen(t, "--datadir", initGzen(t), "export", outfile)
	gzen.WaitExit()
	if have, want := gzen.ExitStatus(), 0; have != want {
		t.Errorf("exit error, have %d want %d", have, want)
	}
	have, err := os.ReadFile(outfile)
	if err != nil {
		t.Fatal(err)
	}
	want := common.FromHex("0xf9026bf90266a00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a08758259b018f7bce3d2be2ddb62f325eaeea0a0c188cf96623eab468a4413e03a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000180837a12008080b875000000000000000000000000000000000000000000000000000000000000000002f0d131f1f97aef08aec6e3291b957d9efe71050000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000880000000000000000c0c0")
	if !bytes.Equal(have, want) {
		t.Fatalf("wrong content exported")
	}
}

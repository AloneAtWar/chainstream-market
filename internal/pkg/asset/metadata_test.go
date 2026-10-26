package asset

import "testing"

func TestParseMetadata(t *testing.T) {
	tests := map[string]struct {
		raw              string
		name, symbol     string
		decimals         int
		hasName, hasDecs bool
	}{
		"erc20":       {raw: `{"Name":{"Value":"Tether","Known":true},"Symbol":{"Value":"USDT","Known":true},"Decimals":{"Value":6,"Known":true}}`, name: "Tether", symbol: "USDT", decimals: 6, hasName: true, hasDecs: true},
		"unknown":     {raw: `{"Name":{"Value":"","Known":false},"Symbol":{"Value":"X","Known":true},"Decimals":{"Value":0,"Known":false}}`, symbol: "X"},
		"native":      {raw: `{"Name":"Ether","Symbol":"ETH","Decimals":18}`, name: "Ether", symbol: "ETH", decimals: 18, hasName: true, hasDecs: true},
		"erc1155":     {raw: `{"Address":"0x1","SupportsMetadataURI":{"Value":true,"Known":true}}`},
		"bad decimal": {raw: `{"Decimals":{"Value":300,"Known":true}}`},
		"empty":       {raw: ``},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			gotName, gotSymbol, gotDecimals := ParseMetadata([]byte(test.raw))
			if (gotName != nil) != test.hasName || (gotName != nil && *gotName != test.name) {
				t.Fatalf("name = %v", gotName)
			}
			if (gotSymbol != nil) != (test.symbol != "") || (gotSymbol != nil && *gotSymbol != test.symbol) {
				t.Fatalf("symbol = %v", gotSymbol)
			}
			if (gotDecimals != nil) != test.hasDecs || (gotDecimals != nil && int(*gotDecimals) != test.decimals) {
				t.Fatalf("decimals = %v", gotDecimals)
			}
		})
	}
}

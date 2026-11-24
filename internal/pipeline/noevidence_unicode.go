package pipeline

/*
UNICODE LICENSE V3

COPYRIGHT AND PERMISSION NOTICE

Copyright © 2001–2026 Unicode, Inc.

NOTICE TO USER: Carefully read the following legal agreement. BY
DOWNLOADING, INSTALLING, COPYING OR OTHERWISE USING DATA FILES, AND/OR
SOFTWARE, YOU UNEQUIVOCALLY ACCEPT, AND AGREE TO BE BOUND BY, ALL OF THE
TERMS AND CONDITIONS OF THIS AGREEMENT. IF YOU DO NOT AGREE, DO NOT
DOWNLOAD, INSTALL, COPY, DISTRIBUTE OR USE THE DATA FILES OR SOFTWARE.

Permission is hereby granted, free of charge, to any person obtaining a
copy of data files and any associated documentation (the "Data Files") or
software and any associated documentation (the "Software") to deal in the
Data Files or Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, and/or sell
copies of the Data Files or Software, and to permit persons to whom the
Data Files or Software are furnished to do so, provided that either (a)
this copyright and permission notice appear with all copies of the Data
Files or Software, or (b) this copyright and permission notice appear in
associated Documentation.

THE DATA FILES AND SOFTWARE ARE PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF
THIRD PARTY RIGHTS.

IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE
BE LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES,
OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION,
ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THE DATA
FILES OR SOFTWARE.

Except as contained in this notice, the name of a copyright holder shall
not be used in advertising or otherwise to promote the sale, use or other
dealings in these Data Files or Software without prior written
authorization of the copyright holder.
*/

// CLDR 49 common/bcp47 records supply the preferred values and well-formed
// BCP 47 type aliases for each Unicode locale extension key.
var unicodeFieldTypeAliases = map[string]map[string]string{
	"ca": {
		"ethiopic-amete-alem": "ethioaa",
		"islamicc":            "islamic-civil",
	},
	"kb": {"yes": "true"},
	"kc": {"yes": "true"},
	"kh": {"yes": "true"},
	"kk": {"yes": "true"},
	"kn": {"yes": "true"},
	"ks": {
		"primary":  "level1",
		"tertiary": "level3",
	},
	"ms": {"imperial": "uksystem"},
	"tz": {
		"aqams":    "aqmcm",
		"aukns":    "auhba",
		"caffs":    "cawnp",
		"camtr":    "cator",
		"canpg":    "cator",
		"capnt":    "caiql",
		"cathu":    "cator",
		"cayzf":    "caedm",
		"cet":      "bebru",
		"cnckg":    "cnsha",
		"cnhrb":    "cnsha",
		"cnkhg":    "cnurc",
		"cuba":     "cuhav",
		"eet":      "grath",
		"egypt":    "egcai",
		"eire":     "iedub",
		"est":      "papty",
		"factory":  "unk",
		"gaza":     "gazastrp",
		"gmt0":     "gmt",
		"hongkong": "hkhkg",
		"hst":      "ushnl",
		"iceland":  "isrey",
		"iran":     "irthr",
		"israel":   "jeruslm",
		"jamaica":  "jmkin",
		"japan":    "jptyo",
		"libya":    "lytip",
		"met":      "bebru",
		"mncoq":    "mnuln",
		"mst":      "usphx",
		"mxstis":   "mxtij",
		"navajo":   "usden",
		"poland":   "plwaw",
		"portugal": "ptlis",
		"prc":      "cnsha",
		"roc":      "twtpe",
		"rok":      "krsel",
		"turkey":   "trist",
		"uaozh":    "uaiev",
		"uauzh":    "uaiev",
		"uct":      "utc",
		"umjon":    "ushnl",
		"usnavajo": "usden",
		"wet":      "ptlis",
		"zulu":     "utc",
	},
}

// CLDR 49 common/bcp47 transform records supply the aliases whose subtags
// fit the transformed-content field value syntax.
var transformFieldTypeAliases = map[string]map[string]string{
	"d0": {"name": "charname"},
	"m0": {
		"beta-metsehaf":  "betamets",
		"ies-jes":        "iesjes",
		"names":          "prprname",
		"tekie-alibekit": "tekieali",
	},
}

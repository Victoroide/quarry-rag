package pipeline

import "strings"

// The tables in this file come from the IANA Language Subtag Registry dated
// 2026-11-18. The pinned language package predates that registry, so they hold
// only what the package lacks: records it does not know and Preferred-Value
// substitutions it does not apply. RFC 5646 section 4.5 requires the latter
// for canonical tags.

// Extended-language prefixes from the registry's extlang records.
var extendedLanguagesByPrefix = map[string]string{
	"ar": `aao abh abv acm acq acw acx acy adf aeb aec afb ajp apc apd arb arq ars
ary arz auz avl ayh ayl ayn ayp bbz pga shu ssh`,
	"kok": "gom knn",
	"lv":  "ltg lvs",
	"ms": `bjn btj bve bvu coa dup hji jak jax kvb kvr kxd lce lcf liw max meo mfa
mfb min mqg msi mui orn ors pel pse tmw urk vkk vkt xmm zlm zmi zsm`,
	"sgn": `ads aed aen afg ajs ase asf asp asq asw bfi bfk bog bqn bqy bvl bzs cds
csc csd cse csf csg csl csn csq csr csx doq dse dsl dsz dyl ecs ehs esl
esn eso eth fcs fse fsl fss gds gse gsg gsm gss gus hab haf hds hks hos
hps hsh hsl icl iks ils inl ins ise isg isr jcs jhs jks jls jos jsl jus
kgi kvk lbs lgs lls lsb lsc lsg lsl lsn lso lsp lst lsv lsw lsy lws mdl
mfs mre msd msr mzc mzg mzy nbs ncs nsi nsl nsp nsr nzs okl pgz pks prl
prz psc psd psg psl pso psp psr pys rib rms rnb rsi rsl rsm rsn sdl sfb
sfs sgg sgx slf sls sqk sqs sqx ssp ssr svk swl syy szs tse tsm tsq tss
tsy tza ugn ugy ukl uks vgt vsi vsl vsv wbs xki xml xms yds ygs yhs ysl
ysm zhk zib zsl`,
	"sw": "swc swh",
	"uz": "uzn uzs",
	"zh": `cdo cjy cmn cnp cpx csp czh czo gan hak hnm hsn luh lzh mnp nan sjc wuu
yue`,
}

func registeredExtendedLanguage(prefix, subtag string) bool {
	for registered := range strings.FieldsSeq(extendedLanguagesByPrefix[prefix]) {
		if registered == subtag {
			return true
		}
	}
	return false
}

// Registered subtags that the pinned language package does not know. A tag
// that uses one cannot be compared by the matcher, so it matches exactly.
var (
	unmatchedLanguages = subtagSet(`cls dyl hnm isv lfb luh oak olb osd rrm scz sjc tvg vsn ynb zhk`)
	unmatchedScripts   = subtagSet(`berf chis gara gukh hntl krai onao seal sidt tayo todr tols tutg`)
	unmatchedVariants  = subtagSet(`1995 2026 akhmimic anpezo blasl bohairic fascia fayyumic fodom
gherd hanoi huett kleinsch leidentr lycopol mdcegyp mdctrans mesokem moderat pehoeji radikalt
sahidic saigon slepe stadi taglish tailo valbadia viennese`)
)

// Preferred values for deprecated records, keyed by lowercase subtag or tag.
var (
	preferredLanguages = map[string]string{
		"bh": "bih", "dek": "sqm", "mo": "ro", "mrd": "mgp", "nte": "eko", "shl": "mrh", "yol": "enm",
	}
	preferredRedundantTags = map[string]string{
		"sgn-br": "bzs", "sgn-co": "csn", "sgn-de": "gsg", "sgn-dk": "dsl", "sgn-es": "ssp",
		"sgn-fr": "fsl", "sgn-gb": "bfi", "sgn-gr": "gss", "sgn-ie": "isg", "sgn-it": "ise",
		"sgn-jp": "jsl", "sgn-mx": "mfs", "sgn-ni": "ncs", "sgn-nl": "dse", "sgn-no": "nsl",
		"sgn-pt": "psr", "sgn-se": "swl", "sgn-us": "ase", "sgn-za": "sfs",
	}
	preferredVariants = map[string]string{"heploc": "alalc97"}
)

func subtagSet(list string) map[string]bool {
	set := make(map[string]bool)
	for subtag := range strings.FieldsSeq(list) {
		set[subtag] = true
	}
	return set
}

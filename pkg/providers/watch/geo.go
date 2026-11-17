package watch

import (
	"regexp"
	"strings"
)

// Every provider names its regions its own way: "us-east-1", "us-central1",
// "newyork-usa-1", "FIN-01", "DE". Geo puts them all into a few parts of the
// world, so that what is sold in one place can be compared across providers.
//
// The dashboard draws its globe and its region filter from the same parts,
// with the same rules, in compute/_helpers/regions.ts. A change here is a
// change there.

// Geos are the parts of the world a region is put in, and GeoOther is the
// part of what cannot be placed.
var Geos = []string{
	"us-east", "us-central", "us-west",
	"ca-east", "ca-west",
	"eu-west", "eu-central", "eu-north",
	"ap-southeast", "ap-northeast", "ap-south",
	"me-west",
}

const GeoOther = "other"

// geoByRegion places the regions whose names say nothing a rule below could
// read, or say it wrongly.
var geoByRegion = map[string]string{
	// Hetzner.
	"ash": "us-east", "hil": "us-west", "fsn1": "eu-central", "nbg1": "eu-central", "hel1": "eu-north", "sin": "ap-southeast",

	// AWS, where the name is not the place.
	"us-east-2":      "us-east",    // Ohio
	"ca-central-1":   "ca-east",    // Montreal
	"eu-south-1":     "eu-central", // Milan
	"eu-south-2":     "eu-west",    // Spain
	"eu-central-2":   "eu-central", // Zurich
	"ap-east-1":      "ap-northeast",
	"ap-east-2":      "ap-northeast",
	"ap-southeast-5": "ap-southeast",
	"ap-southeast-7": "ap-southeast",
	"me-south-1":     "me-west",
	"me-central-1":   "me-west",
	"il-central-1":   "me-west",

	// GCP.
	"us-south1":               "us-central", // Dallas
	"us-east5":                "us-east",    // Columbus
	"northamerica-northeast1": "ca-east",
	"northamerica-northeast2": "ca-east",
	"europe-west1":            "eu-west",    // Belgium
	"europe-west2":            "eu-west",    // London
	"europe-west3":            "eu-central", // Frankfurt
	"europe-west4":            "eu-west",    // Netherlands
	"europe-west6":            "eu-central", // Zurich
	"europe-west8":            "eu-central", // Milan
	"europe-west9":            "eu-west",    // Paris
	"europe-west10":           "eu-central", // Berlin
	"europe-west12":           "eu-central", // Turin
	"europe-central2":         "eu-central", // Warsaw
	"europe-north1":           "eu-north",
	"europe-north2":           "eu-north",
	"europe-southwest1":       "eu-west", // Madrid
	"asia-east1":              "ap-northeast",
	"asia-east2":              "ap-northeast",
	"asia-northeast1":         "ap-northeast",
	"asia-northeast2":         "ap-northeast",
	"asia-northeast3":         "ap-northeast",
	"asia-south1":             "ap-south",
	"asia-south2":             "ap-south",
	"asia-southeast1":         "ap-southeast",
	"asia-southeast2":         "ap-southeast",
	"australia-southeast1":    "ap-southeast",
	"australia-southeast2":    "ap-southeast",
	"me-central1":             "me-west",
	"me-central2":             "me-west",
	"me-west1":                "me-west",

	// Nebius.
	"uk-south1": "eu-west",
}

// A region named for where it is, as AWS, GCP and Nebius name theirs:
// "us-east-1", "us-central1", "eu-north1".
var compassRegion = regexp.MustCompile(`^([a-z]+)-(north|south|east|west|central|northeast|southeast|southwest|northwest)-?\d+$`)

var geoByCompass = map[string]string{
	"us-east": "us-east", "us-central": "us-central", "us-west": "us-west",
	"ca-east": "ca-east", "ca-central": "ca-east", "ca-west": "ca-west",
	"eu-west": "eu-west", "eu-central": "eu-central", "eu-north": "eu-north", "eu-south": "eu-central",
	"ap-southeast": "ap-southeast", "ap-northeast": "ap-northeast", "ap-south": "ap-south",
	"me-west": "me-west", "me-central": "me-west", "me-south": "me-west",
}

// geoByCity places a region that names a city or another place.
var geoByCity = map[string]string{
	// US East
	"newyork": "us-east", "nyc": "us-east", "washington": "us-east", "virginia": "us-east", "ashburn": "us-east",
	"atlanta": "us-east", "miami": "us-east", "boston": "us-east", "reston": "us-east", "newark": "us-east",
	"ohio": "us-east", "columbus": "us-east", "carolina": "us-east", "florida": "us-east", "georgia": "us-east",
	"pennsylvania": "us-east", "newjersey": "us-east", "ash": "us-east",
	// US Central
	"desmoines": "us-central", "kansascity": "us-central", "chicago": "us-central", "dallas": "us-central",
	"texas": "us-central", "iowa": "us-central", "omaha": "us-central", "denver": "us-central", "houston": "us-central",
	"stlouis": "us-central", "minneapolis": "us-central", "illinois": "us-central", "colorado": "us-central",
	"kansas": "us-central", "missouri": "us-central", "nebraska": "us-central", "oklahoma": "us-central",
	"utah": "us-central", "saltlakecity": "us-central",
	// US West
	"sanfrancisco": "us-west", "sanjose": "us-west", "losangeles": "us-west", "seattle": "us-west", "portland": "us-west",
	"oregon": "us-west", "phoenix": "us-west", "lasvegas": "us-west", "siliconvalley": "us-west", "fremont": "us-west",
	"santaclara": "us-west", "hillsboro": "us-west", "hil": "us-west", "california": "us-west", "nevada": "us-west",
	"arizona": "us-west",
	// Canada
	"montreal": "ca-east", "toronto": "ca-east", "quebec": "ca-east", "ottawa": "ca-east", "ontario": "ca-east",
	"calgary": "ca-west", "vancouver": "ca-west", "edmonton": "ca-west", "alberta": "ca-west",
	"britishcolumbia": "ca-west",
	// EU West
	"paris": "eu-west", "london": "eu-west", "dublin": "eu-west", "amsterdam": "eu-west", "marseille": "eu-west",
	"madrid": "eu-west", "brussels": "eu-west", "lisbon": "eu-west",
	// EU Central
	"frankfurt": "eu-central", "berlin": "eu-central", "warsaw": "eu-central", "munich": "eu-central", "zurich": "eu-central",
	"vienna": "eu-central", "milan": "eu-central", "prague": "eu-central", "falkenstein": "eu-central",
	"nuremberg": "eu-central",
	// EU North
	"stockholm": "eu-north", "helsinki": "eu-north", "oslo": "eu-north", "copenhagen": "eu-north",
	"reykjavik": "eu-north",
	// APAC
	"singapore": "ap-southeast", "jakarta": "ap-southeast", "sydney": "ap-southeast", "melbourne": "ap-southeast",
	"tokyo": "ap-northeast", "osaka": "ap-northeast", "seoul": "ap-northeast", "hongkong": "ap-northeast",
	"taipei": "ap-northeast",
	"mumbai": "ap-south", "delhi": "ap-south", "bangalore": "ap-south", "chennai": "ap-south", "hyderabad": "ap-south",
	// Middle East
	"dubai": "me-west", "bahrain": "me-west", "telaviv": "me-west", "doha": "me-west",
}

// geoByCountry places a region that names no more than a country, by its
// name or by the two or three letters that stand for it. A country as wide
// as the United States is put in its middle.
var geoByCountry = map[string]string{
	"usa": "us-central", "us": "us-central", "unitedstates": "us-central",
	"canada": "ca-east", "ca": "ca-east", "can": "ca-east",

	"france": "eu-west", "fr": "eu-west", "fra": "eu-west",
	"uk": "eu-west", "gb": "eu-west", "gbr": "eu-west", "unitedkingdom": "eu-west",
	"ireland": "eu-west", "ie": "eu-west", "irl": "eu-west",
	"netherlands": "eu-west", "nl": "eu-west", "nld": "eu-west",
	"spain": "eu-west", "es": "eu-west", "esp": "eu-west",
	"portugal": "eu-west", "pt": "eu-west", "prt": "eu-west",
	"belgium": "eu-west", "be": "eu-west", "bel": "eu-west",
	"luxembourg": "eu-west", "lu": "eu-west",

	"germany": "eu-central", "de": "eu-central", "deu": "eu-central", "ger": "eu-central",
	"poland": "eu-central", "pl": "eu-central", "pol": "eu-central",
	"switzerland": "eu-central", "ch": "eu-central", "che": "eu-central",
	"austria": "eu-central", "at": "eu-central", "aut": "eu-central",
	"italy": "eu-central", "it": "eu-central", "ita": "eu-central",
	"czechia": "eu-central", "cz": "eu-central", "cze": "eu-central",
	"hungary": "eu-central", "hu": "eu-central",
	"romania": "eu-central", "ro": "eu-central",
	"bulgaria": "eu-central", "bg": "eu-central",
	"slovakia": "eu-central", "sk": "eu-central",
	"slovenia": "eu-central", "si": "eu-central",
	"croatia": "eu-central", "hr": "eu-central",
	"serbia": "eu-central", "rs": "eu-central",
	"greece": "eu-central", "gr": "eu-central",
	"ukraine": "eu-central", "ua": "eu-central",
	"moldova": "eu-central", "md": "eu-central",

	"sweden": "eu-north", "se": "eu-north", "swe": "eu-north",
	"finland": "eu-north", "fi": "eu-north", "fin": "eu-north",
	"norway": "eu-north", "no": "eu-north", "nor": "eu-north",
	"denmark": "eu-north", "dk": "eu-north", "dnk": "eu-north",
	"iceland": "eu-north", "is": "eu-north", "ice": "eu-north", "isl": "eu-north",
	"estonia": "eu-north", "ee": "eu-north",
	"latvia": "eu-north", "lv": "eu-north",
	"lithuania": "eu-north", "lt": "eu-north",

	"singapore": "ap-southeast", "sg": "ap-southeast", "sgp": "ap-southeast",
	"indonesia": "ap-southeast", "id": "ap-southeast",
	"australia": "ap-southeast", "au": "ap-southeast", "aus": "ap-southeast",
	"newzealand": "ap-southeast", "nz": "ap-southeast",
	"malaysia": "ap-southeast", "my": "ap-southeast",
	"thailand": "ap-southeast", "th": "ap-southeast",
	"vietnam": "ap-southeast", "vn": "ap-southeast",
	"philippines": "ap-southeast", "ph": "ap-southeast",

	"japan": "ap-northeast", "jp": "ap-northeast", "jpn": "ap-northeast",
	"korea": "ap-northeast", "southkorea": "ap-northeast", "kr": "ap-northeast", "kor": "ap-northeast",
	"taiwan": "ap-northeast", "tw": "ap-northeast",
	"hk":    "ap-northeast",
	"china": "ap-northeast", "cn": "ap-northeast",

	"india": "ap-south", "in": "ap-south", "ind": "ap-south",

	"uae": "me-west", "ae": "me-west",
	"israel": "me-west", "il": "me-west", "isr": "me-west",
	"bahrain": "me-west", "bh": "me-west",
	"saudiarabia": "me-west", "sa": "me-west",
	"qatar": "me-west", "qa": "me-west",
	"turkey": "me-west", "tr": "me-west",
}

var regionWords = regexp.MustCompile(`[-_/\s,]+`)

// Geo is the part of the world a provider's region is in, or GeoOther for a
// region that cannot be placed.
func Geo(region string) string {
	code := strings.ToLower(strings.TrimSpace(region))
	// A region that is one of the parts already is itself. Read word by
	// word, "us-west" would be the United States, and so its middle.
	for _, geo := range Geos {
		if code == geo {
			return geo
		}
	}
	if geo, ok := geoByRegion[code]; ok {
		return geo
	}
	if named := compassRegion.FindStringSubmatch(code); named != nil {
		if geo, ok := geoByCompass[named[1]+"-"+named[2]]; ok {
			return geo
		}
		// Named like a region of a hyperscaler, for a part of the world
		// that is none of these: South America, Africa.
		return GeoOther
	}

	words := regionWords.Split(code, -1)
	for _, word := range words {
		if geo, ok := geoByCity[word]; ok {
			return geo
		}
	}
	for _, word := range words {
		if geo, ok := geoByCountry[word]; ok {
			return geo
		}
	}
	return GeoOther
}

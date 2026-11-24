// Package wordprocessingml defines the schema identifiers used by Word packages.
package wordprocessingml

import "net/url"

const (
	openXMLHost   = "schemas.openxmlformats.org"
	strictHost    = "purl.oclc.org"
	microsoftHost = "schemas.microsoft.com"
	xmlHost       = "www.w3.org"
)

// Namespaces identify supported package and document vocabularies.
var (
	Word       = schemaURI(openXMLHost, "/wordprocessingml/2006/main")
	StrictWord = schemaURI(strictHost, "/ooxml/wordprocessingml/main")

	OfficeRelationships       = schemaURI(openXMLHost, "/officeDocument/2006/relationships")
	StrictOfficeRelationships = schemaURI(strictHost, "/ooxml/officeDocument/relationships")
	PackageRelationships      = schemaURI(openXMLHost, "/package/2006/relationships")

	Math       = schemaURI(openXMLHost, "/officeDocument/2006/math")
	StrictMath = schemaURI(strictHost, "/ooxml/officeDocument/math")

	Compatibility = schemaURI(openXMLHost, "/markup-compatibility/2006")
	WordShape     = schemaURI(microsoftHost, "/office/word/2010/wordprocessingShape")

	XML   = schemaURI(xmlHost, "/XML/1998/namespace")
	XMLNS = schemaURI(xmlHost, "/2000/xmlns/")
)

func schemaURI(host, path string) string {
	uri := url.URL{Scheme: "http", Host: host, Path: path}
	return uri.String()
}

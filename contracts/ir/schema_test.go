package ircontract_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v5"

	ircontract "github.com/uu999/evalfrog/contracts/ir"
	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/ir"
)

type fixtureManifest struct {
	Valid     []string           `json:"valid"`
	Canonical []canonicalFixture `json:"canonical"`
	Invalid   []invalidFixture   `json:"invalid"`
}

type canonicalFixture struct {
	Input  string `json:"input"`
	Golden string `json:"golden"`
}

type invalidFixture struct {
	File        string   `json:"file"`
	SchemaValid bool     `json:"schema_valid"`
	Stage       string   `json:"stage"`
	Codes       []string `json:"codes"`
}

func TestIRV1FixturesAgainstSchemaAndGoContracts(t *testing.T) {
	t.Parallel()
	schema := compileSchema(t)
	manifest := readManifest(t)
	parser := ir.DefaultParser()
	validator := ir.NewStrictValidator(catalog.BuiltinV1())

	for _, name := range manifest.Valid {
		name := name
		t.Run("valid/"+name, func(t *testing.T) {
			raw := readFixture(t, name)
			if err := schema.Validate(decodeJSON(t, raw)); err != nil {
				t.Fatalf("JSON Schema rejected valid fixture: %v", err)
			}
			document, diagnostics := parser.ParseDraft(raw)
			if ir.HasErrors(diagnostics) {
				t.Fatalf("parser rejected valid fixture: %+v", diagnostics)
			}
			if diagnostics = validator.Validate(document); ir.HasErrors(diagnostics) {
				t.Fatalf("strict validator rejected valid fixture: %+v", diagnostics)
			}
			expectedBytes, expectedHash, err := ir.CanonicalDocumentHash(document)
			if err != nil {
				t.Fatal(err)
			}
			for iteration := 0; iteration < 100; iteration++ {
				actualBytes, actualHash, hashErr := ir.CanonicalDocumentHash(document)
				if hashErr != nil || !bytes.Equal(expectedBytes, actualBytes) || expectedHash != actualHash {
					t.Fatalf("canonical output changed at iteration %d: %v", iteration, hashErr)
				}
			}
		})
	}

	for _, fixture := range manifest.Invalid {
		fixture := fixture
		t.Run("invalid/"+fixture.File, func(t *testing.T) {
			raw := readFixture(t, fixture.File)
			schemaErr := schema.Validate(decodeJSON(t, raw))
			if (schemaErr == nil) != fixture.SchemaValid {
				t.Fatalf("schema_valid=%v, schema error=%v", fixture.SchemaValid, schemaErr)
			}
			document, diagnostics := parser.ParseDraft(raw)
			if fixture.Stage == "parse" {
				assertCodes(t, diagnostics, fixture.Codes)
				return
			}
			if ir.HasErrors(diagnostics) {
				t.Fatalf("fixture intended for validator failed parsing: %+v", diagnostics)
			}
			assertCodes(t, validator.Validate(document), fixture.Codes)
		})
	}
}

func TestSchemaDataTypesMatchGoModelAndCatalog(t *testing.T) {
	t.Parallel()
	var schema map[string]any
	if err := json.Unmarshal(ircontract.SchemaV1(), &schema); err != nil {
		t.Fatal(err)
	}
	definitions := schema["$defs"].(map[string]any)
	dataType := definitions["dataType"].(map[string]any)
	values := dataType["enum"].([]any)
	actual := make([]string, 0, len(values))
	for _, value := range values {
		actual = append(actual, value.(string))
	}
	expected := make([]string, 0, len(ir.AllDataTypes()))
	for _, value := range ir.AllDataTypes() {
		expected = append(expected, string(value))
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("schema types %v differ from Go types %v", actual, expected)
	}

	seen := make(map[ir.DataType]bool)
	for _, description := range catalog.BuiltinV1().Descriptions() {
		for _, input := range description.Inputs {
			for _, dataType := range input.DataTypes {
				seen[dataType] = true
			}
		}
		if description.AdditionalInputs != nil {
			for _, dataType := range description.AdditionalInputs.DataTypes {
				seen[dataType] = true
			}
		}
		for _, dataType := range description.Outputs.AllowedDataTypes {
			seen[dataType] = true
		}
		for _, output := range description.Outputs.Fields {
			seen[output.DataType] = true
		}
	}
	for _, dataType := range ir.AllDataTypes() {
		if !seen[dataType] {
			t.Fatalf("catalog never describes IR data type %s", dataType)
		}
	}
}

func TestCanonicalGoldenFixtures(t *testing.T) {
	t.Parallel()
	manifest := readManifest(t)
	for _, fixture := range manifest.Canonical {
		fixture := fixture
		t.Run(fixture.Input, func(t *testing.T) {
			actual, err := ir.CanonicalizeJSON(readFixture(t, fixture.Input), ir.DefaultParseLimits)
			if err != nil {
				t.Fatal(err)
			}
			expected := bytes.TrimSpace(readFixture(t, fixture.Golden))
			if !bytes.Equal(actual, expected) {
				t.Fatalf("canonical contract changed:\nactual:   %s\nexpected: %s", actual, expected)
			}
		})
	}
}

func TestEmbeddedSchemaReturnsDefensiveCopy(t *testing.T) {
	t.Parallel()
	first := ircontract.SchemaV1()
	first[0] = 'x'
	if ircontract.SchemaV1()[0] != '{' {
		t.Fatal("caller mutated embedded schema")
	}
}

func TestIRV2SchemaAndCatalog(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020
	const uri = "https://evalfrog.dev/contracts/ir/v2/schema.json"
	if err := compiler.AddResource(uri, bytes.NewReader(ircontract.SchemaV2())); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile(uri)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"ir_version":"2","nodes":[{"id":"start","type":"start","title":"Start","inputs":[],"outputs":[{"name":"amount","data_type":"integer"}]},{"id":"end","type":"end","title":"End","inputs":[{"name":"total","data_type":"integer","source":"ref","ref_node":"start","ref_output":"amount"}],"outputs":[{"name":"total","data_type":"integer"}]}],"edges":[{"id":"flow","source":"start","target":"end"}],"layout":{"start":{"x":0,"y":0},"end":{"x":200,"y":0}}}`)
	if err := schema.Validate(decodeJSON(t, raw)); err != nil {
		t.Fatal(err)
	}
	document, diagnostics := ir.DefaultParser().ParseDraft(raw)
	if ir.HasErrors(diagnostics) {
		t.Fatalf("parse: %+v", diagnostics)
	}
	if diagnostics := ir.NewStrictValidator(catalog.BuiltinV2()).Validate(document); ir.HasErrors(diagnostics) {
		t.Fatalf("validate: %+v", diagnostics)
	}
	legacy := bytes.Replace(raw, []byte(`"ir_version":"2"`), []byte(`"ir_version":"1"`), 1)
	if err := schema.Validate(decodeJSON(t, legacy)); err == nil {
		t.Fatal("v2 schema accepted v1 IR")
	}
}

func compileSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020
	if err := compiler.AddResource("https://evalfrog.dev/contracts/ir/v1/schema.json", bytes.NewReader(ircontract.SchemaV1())); err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.Compile("https://evalfrog.dev/contracts/ir/v1/schema.json")
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func readManifest(t *testing.T) fixtureManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("v1", "fixtures", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest fixtureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("v1", "fixtures", filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func decodeJSON(t *testing.T, raw []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func assertCodes(t *testing.T, diagnostics []ir.Diagnostic, expected []string) {
	t.Helper()
	actual := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		actual = append(actual, diagnostic.Code)
	}
	sort.Strings(actual)
	for _, code := range expected {
		position := sort.SearchStrings(actual, code)
		if position == len(actual) || actual[position] != code {
			t.Fatalf("diagnostic %s not found in %v", code, actual)
		}
	}
}

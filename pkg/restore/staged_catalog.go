package restore

// StagedCatalog is an exclusively owned, authenticated host preparation input.
// Direct callers transfer ownership to one slot. Mutable runtime/disk objects
// are intentionally absent. JSON remains supported for legacy process transport.
type StagedCatalog struct {
	Ref            string                      `json:"ref"`
	SandboxHint    string                      `json:"sandbox_hint,omitempty"`
	Locations      map[string]string           `json:"locations"`
	ManifestConfig string                      `json:"manifest_config"`
	Key            string                      `json:"key"`
	Files          map[string]StageFileVersion `json:"files"`
}

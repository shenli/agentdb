module github.com/shenli/agentdb/examples/agents/go

go 1.22

// While agentdb is unpublished, point at the local SDK source.
// Once a release lands, drop this replace and pin a version in
// the require block above.
require github.com/shenli/agentdb/clients/go v0.0.0

replace github.com/shenli/agentdb/clients/go => ../../../clients/go

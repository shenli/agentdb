package persona

type Persona struct {
	TenantID              string  `yaml:"tenant_id"`
	TicketRatePerMin      float64 `yaml:"ticket_rate_per_min"`
	ConcurrentSessionsCap int     `yaml:"concurrent_sessions_cap"`
	MessageIntervalMs     [2]int  `yaml:"message_interval_ms"`
	SessionLen            [2]int  `yaml:"session_len"`
	EscalationProb        float64 `yaml:"escalation_prob"`
	MemoryRecallProb      float64 `yaml:"memory_recall_prob"`
}

type Config struct {
	MongoURI    string    `yaml:"mongo_uri"`
	RESTBase    string    `yaml:"rest_base"`
	ControlAddr string    `yaml:"control_addr"`
	Personas    []Persona `yaml:"personas"`
}

package siesta

type Metadata struct{}

func NewMetadata() *Metadata {
	return &Metadata{}
}

func (m *Metadata) requestUpdate() {
}

func (m *Metadata) timeToNextUpdate(int64) int64 {
	return 0
}

func (m *Metadata) refreshBackoff() int64 {
	return 0
}

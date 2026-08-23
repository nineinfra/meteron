package provider

import (
	"testing"
	"time"

	"github.com/meteron/meteron/internal/config"
)

func TestRegistryLocalProvider(t *testing.T) {
	r, err := NewRegistry([]config.ProviderConfig{
		{Name: "ollama", Type: "local", BaseURL: "http://localhost:11434/v1", Models: []string{"llama3.1"}},
		{Name: "vllm", Type: "local", BaseURL: "http://localhost:8000/v1", Models: []string{"qwen2.5-72b"}},
	}, time.Second)
	if err != nil {
		t.Fatalf("local providers rejected: %v", err)
	}
	if p, ok := r.ProviderForModel("llama3.1"); !ok || p != "ollama" {
		t.Fatalf("ProviderForModel(llama3.1) = %q,%v, want ollama", p, ok)
	}
	if p, ok := r.ProviderForModel("qwen2.5-72b"); !ok || p != "vllm" {
		t.Fatalf("ProviderForModel(qwen2.5-72b) = %q,%v, want vllm", p, ok)
	}
}

func TestRegistryLocalRequiresBaseURL(t *testing.T) {
	if _, err := NewRegistry([]config.ProviderConfig{{Name: "x", Type: "local"}}, time.Second); err == nil {
		t.Fatal("local without base_url should fail")
	}
}

func TestRegistryUnknownType(t *testing.T) {
	if _, err := NewRegistry([]config.ProviderConfig{{Name: "x", Type: "bogus", BaseURL: "http://x"}}, time.Second); err == nil {
		t.Fatal("unknown provider type should fail")
	}
}

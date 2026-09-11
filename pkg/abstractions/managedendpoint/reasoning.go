package managedendpoint

import (
	"fmt"
	"slices"
)

// normalizeReasoning maps OpenRouter's documented effort/enable controls to
// the engine's OpenAI schema. Unsupported budgets/exclusion must fail visibly
// instead of silently generating (and charging for) unwanted reasoning.
func normalizeReasoning(payload map[string]any) error {
	raw, exists := payload["reasoning"]
	if !exists {
		return nil
	}
	reasoning, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("reasoning must be an object")
	}
	effort := ""
	if raw, exists := reasoning["effort"]; exists {
		effort, ok = raw.(string)
		if !ok || !slices.Contains([]string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}, effort) {
			return fmt.Errorf("reasoning.effort is invalid")
		}
	}
	for key, value := range reasoning {
		switch key {
		case "enabled":
			enabled, ok := value.(bool)
			if !ok {
				return fmt.Errorf("reasoning.enabled must be a boolean")
			}
			if !enabled {
				if effort != "" && effort != "none" {
					return fmt.Errorf("reasoning.enabled conflicts with reasoning.effort")
				}
				effort = "none"
			} else {
				if effort == "none" {
					return fmt.Errorf("reasoning.enabled conflicts with reasoning.effort")
				}
				if effort == "" {
					effort = "medium"
				}
			}
		case "effort":
		case "exclude":
			if value != false {
				return fmt.Errorf("reasoning.exclude=true is not supported")
			}
		default:
			return fmt.Errorf("reasoning.%s is not supported; use reasoning.effort or reasoning.enabled", key)
		}
	}
	if effort != "" {
		if existing, ok := payload["reasoning_effort"]; ok && existing != effort {
			return fmt.Errorf("reasoning conflicts with reasoning_effort")
		}
		payload["reasoning_effort"] = effort
	}
	delete(payload, "reasoning")
	return nil
}

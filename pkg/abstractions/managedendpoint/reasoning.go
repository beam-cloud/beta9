package managedendpoint

import (
	"fmt"
	"slices"
)

// normalizeReasoning maps OpenRouter's documented effort/enable controls to
// the engine's OpenAI schema. Unsupported budgets/exclusion must fail visibly
// instead of silently generating (and charging for) unwanted reasoning.
func normalizeReasoning(payload map[string]any) (bool, error) {
	raw, exists := payload["reasoning"]
	if !exists {
		return false, nil
	}
	reasoning, ok := raw.(map[string]any)
	if !ok {
		return false, fmt.Errorf("reasoning must be an object")
	}
	effort := ""
	if raw, exists := reasoning["effort"]; exists {
		effort, ok = raw.(string)
		if !ok || !slices.Contains([]string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}, effort) {
			return false, fmt.Errorf("reasoning.effort is invalid")
		}
	}
	for key, value := range reasoning {
		switch key {
		case "enabled":
			enabled, ok := value.(bool)
			if !ok {
				return false, fmt.Errorf("reasoning.enabled must be a boolean")
			}
			if !enabled {
				if effort != "" && effort != "none" {
					return false, fmt.Errorf("reasoning.enabled conflicts with reasoning.effort")
				}
				effort = "none"
			} else {
				if effort == "none" {
					return false, fmt.Errorf("reasoning.enabled conflicts with reasoning.effort")
				}
				if effort == "" {
					effort = "medium"
				}
			}
		case "effort":
		case "exclude":
			if value != false {
				return false, fmt.Errorf("reasoning.exclude=true is not supported")
			}
		default:
			return false, fmt.Errorf("reasoning.%s is not supported; use reasoning.effort or reasoning.enabled", key)
		}
	}
	if effort != "" {
		if existing, ok := payload["reasoning_effort"]; ok && existing != effort {
			return false, fmt.Errorf("reasoning conflicts with reasoning_effort")
		}
		payload["reasoning_effort"] = effort
	}
	delete(payload, "reasoning")
	return true, nil
}

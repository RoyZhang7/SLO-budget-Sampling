package slobudget

import (
	"fmt"
	"math"
)

// Config is sampler.BudgetConfig with the same field names, defaults and
// validation.  The Stage 2 settings are fixed (local request budget, uniform
// plan weights, escalation off, class key on, on-demand roots, frozen
// calibration on the rate trigger, relative N_need, credit off), but the knobs
// the simulator swept are kept so that a replay can be configured like any
// Python run.  Mechanisms that are off in Stage 2 (escalation, keep_parent,
// credit / reserve, the record root policy) are not ported and are rejected
// by Validate.
type Config struct {
	NNeed                float64 `json:"n_need" mapstructure:"n_need"`
	NNeedMode            string  `json:"n_need_mode" mapstructure:"n_need_mode"`
	NNeedFrac            float64 `json:"n_need_frac" mapstructure:"n_need_frac"`
	NMin                 int     `json:"n_min" mapstructure:"n_min"`
	PRule                string  `json:"p_rule" mapstructure:"p_rule"`
	Alpha                float64 `json:"alpha" mapstructure:"alpha"`
	CalibQ               float64 `json:"calib_q" mapstructure:"calib_q"`
	CalibWindow          int     `json:"calib_window" mapstructure:"calib_window"`
	MinClassSamples      int     `json:"min_class_samples" mapstructure:"min_class_samples"`
	UseClass             bool    `json:"use_class" mapstructure:"use_class"`
	Plan                 string  `json:"plan" mapstructure:"plan"`
	PlanFill             float64 `json:"plan_fill" mapstructure:"plan_fill"`
	Escalation           string  `json:"escalation" mapstructure:"escalation"`
	EscalateBypassBudget bool    `json:"escalate_bypass_budget" mapstructure:"escalate_bypass_budget"`
	BudgetScope          string  `json:"budget_scope" mapstructure:"budget_scope"`
	KeepParent           bool    `json:"keep_parent" mapstructure:"keep_parent"`
	Unbounded            bool    `json:"unbounded" mapstructure:"unbounded"`
	CalibSource          string  `json:"calib_source" mapstructure:"calib_source"`
	FreezeTrigger        string  `json:"freeze_trigger" mapstructure:"freeze_trigger"`
	FreezeRateMult       float64 `json:"freeze_rate_mult" mapstructure:"freeze_rate_mult"`
	FreezeRateFloor      float64 `json:"freeze_rate_floor" mapstructure:"freeze_rate_floor"`
	FreezeEwma           float64 `json:"freeze_ewma" mapstructure:"freeze_ewma"`
	ColdPrior            string  `json:"cold_prior" mapstructure:"cold_prior"`
	RootPolicy           string  `json:"root_policy" mapstructure:"root_policy"`
	RootFlagOn           string  `json:"root_flag_on" mapstructure:"root_flag_on"`
	RecordCost           float64 `json:"record_cost" mapstructure:"record_cost"`
	OutlierCredit        string  `json:"outlier_credit" mapstructure:"outlier_credit"`
	InitialBudget        float64 `json:"initial_budget" mapstructure:"initial_budget"`
	InitialPlanP         float64 `json:"initial_plan_p" mapstructure:"initial_plan_p"`
	PoolFactor           float64 `json:"pool_factor" mapstructure:"pool_factor"`
	BMin                 float64 `json:"b_min" mapstructure:"b_min"`
	BMax                 float64 `json:"b_max" mapstructure:"b_max"`
	RateEwma             float64 `json:"rate_ewma" mapstructure:"rate_ewma"`
	OrdinaryFloor        float64 `json:"ordinary_floor" mapstructure:"ordinary_floor"`
	// OutlierMargin m: a span is an outlier only when its duration exceeds (1 + m) T_i (or its
	// status is an error); spans between T_i and (1 + m) T_i are ordinary.  Stage 2c default 0.1.
	OutlierMargin float64 `json:"outlier_margin" mapstructure:"outlier_margin"`
}

// DefaultConfig is BudgetConfig() with the Python defaults.
func DefaultConfig() Config {
	return Config{
		NNeed: 15.0, NNeedMode: "fixed", NNeedFrac: 0.2, NMin: 3, PRule: "linear", Alpha: 0.5, CalibQ: 0.95,
		CalibWindow: 2, MinClassSamples: 20, UseClass: true, Plan: "uniform", PlanFill: 0.5, Escalation: "none",
		BudgetScope: "shared", CalibSource: "kept", FreezeTrigger: "rate", FreezeRateMult: 2.0, FreezeRateFloor: 0.01,
		FreezeEwma: 0.3, ColdPrior: "none", RootPolicy: "pin", RootFlagOn: "outlier", RecordCost: 0.1,
		OutlierCredit: "none", InitialBudget: 3.0, InitialPlanP: 0.15, PoolFactor: 1.0, BMin: 0.05, BMax: 64.0,
		RateEwma: 0.5, OrdinaryFloor: 0.05, OutlierMargin: 0.1,
	}
}

// ChosenV2 is the `chosen-v2` preset of sampler.make_budget_arm as of pass 4:
// the Stage 2 configuration.
func ChosenV2() Config {
	c := DefaultConfig()
	c.BudgetScope = "local"
	c.Plan = "uniform"
	c.Escalation = "none"
	c.UseClass = true
	c.RootPolicy = "on_demand"
	c.CalibSource = "frozen"
	c.NNeedMode = "relative"
	c.NNeedFrac = 0.2
	c.OutlierCredit = "none"
	c.ColdPrior = "none"
	return c
}

// Validate mirrors BudgetConfig.__post_init__ and rejects the mechanisms the
// Go port leaves out.
func (c *Config) Validate() error {
	if c.NNeed >= 1e300 { // the dump writes inf as 1e300 (JSON has no inf)
		c.NNeed = math.Inf(1)
	}
	if c.OutlierMargin < 0 {
		return fmt.Errorf("outlier_margin must be >= 0")
	}
	switch c.CalibSource {
	case "kept", "plan", "frozen", "oracle":
	default:
		return fmt.Errorf("unknown calib_source %q; choose from kept, plan, frozen, oracle", c.CalibSource)
	}
	switch c.RootPolicy {
	case "pin", "on_demand", "plan":
	case "record":
		return fmt.Errorf("root_policy record is not ported to Go (Stage 2 uses on_demand)")
	default:
		return fmt.Errorf("unknown root_policy %q", c.RootPolicy)
	}
	if c.RootFlagOn != "outlier" && c.RootFlagOn != "any" {
		return fmt.Errorf("unknown root_flag_on %q", c.RootFlagOn)
	}
	if c.OutlierCredit != "none" {
		return fmt.Errorf("outlier_credit %q is not ported to Go (Stage 2 decision: credit and reserve off)", c.OutlierCredit)
	}
	if c.NNeedMode != "fixed" && c.NNeedMode != "relative" {
		return fmt.Errorf("unknown n_need_mode %q", c.NNeedMode)
	}
	if c.FreezeTrigger != "rate" && c.FreezeTrigger != "count" {
		return fmt.Errorf("unknown freeze_trigger %q", c.FreezeTrigger)
	}
	switch c.ColdPrior {
	case "none", "equal", "file":
	default:
		return fmt.Errorf("unknown cold_prior %q; choose from none, equal, file", c.ColdPrior)
	}
	if c.Escalation != "none" {
		return fmt.Errorf("escalation %q is not ported to Go (Stage 2 setting: off)", c.Escalation)
	}
	if c.KeepParent {
		return fmt.Errorf("keep_parent is not ported to Go")
	}
	if c.BudgetScope != "shared" && c.BudgetScope != "local" {
		return fmt.Errorf("unknown budget_scope %q", c.BudgetScope)
	}
	if c.Plan != "uniform" && c.Plan != "sqrt" {
		return fmt.Errorf("unknown plan %q", c.Plan)
	}
	if _, err := growth(c.PRule); err != nil {
		return err
	}
	return nil
}

func growth(rule string) (func(miss float64) float64, error) {
	switch rule {
	case "const":
		return func(float64) float64 { return 1.0 }, nil
	case "linear":
		return func(miss float64) float64 { return 1.0 + miss }, nil
	case "sqrt":
		return func(miss float64) float64 { return 1.0 + math.Sqrt(miss) }, nil
	case "quadratic":
		return func(miss float64) float64 { return (1.0 + miss) * (1.0 + miss) }, nil
	}
	return nil, fmt.Errorf("unknown p rule %q", rule)
}

package execengine2

import (
	"errors"
	"fmt"
	"time"
)

// Config хранит только правила исполнения.
// Клиент брокера и сброс лимита передаются отдельно.
type Config struct {
	LegA string
	LegB string

	Lots  int
	Mode  Mode
	Ratio int

	BookMaxAge               time.Duration
	PriceWait                time.Duration
	MinRest                  time.Duration
	TradeTimeout             time.Duration
	ForceCloseOnTimeout      bool
	KeepPartialOpenOnTimeout bool
	DisableRepeg             bool
	PullOnStaleBook          bool
	RetryWait                time.Duration
	RetryMax                 time.Duration
	MarketCheckAfter         time.Duration
	MarketCheckEvery         time.Duration
	// HedgeTries bounds ordinary attempts at the remaining hedge size. When
	// RejectRetryLotStep is positive, these follow the finite shrinking ladder.
	HedgeTries int
	// RejectRetryLotStep enables smaller retries after definitive rejections.
	// Opening entries keep their requested size; mandatory hedges accumulate
	// accepted chunks toward the full obligation. Zero disables the ladder.
	RejectRetryLotStep int
	RejectRetryMinLots int

	LogTag string
}

func (c Config) normalized() Config {
	if c.Mode == ModeDefault {
		c.Mode = ModeTwoLimits
	}
	if c.Ratio == 0 {
		c.Ratio = 1
	}
	if c.PriceWait == 0 {
		c.PriceWait = 500 * time.Millisecond
	}
	if c.RetryWait == 0 {
		c.RetryWait = 2 * time.Second
	}
	if c.RetryMax == 0 {
		c.RetryMax = time.Minute
	}
	if c.MarketCheckAfter == 0 {
		c.MarketCheckAfter = 10 * time.Second
	}
	if c.MarketCheckEvery == 0 {
		c.MarketCheckEvery = 3 * time.Second
	}
	if c.HedgeTries == 0 {
		c.HedgeTries = 1
	}
	if c.RejectRetryMinLots == 0 {
		c.RejectRetryMinLots = 1
	}
	return c
}

func (c Config) validate() error {
	if c.LegA == "" || c.LegB == "" {
		return errors.New("both pair symbols are required")
	}
	if c.LegA == c.LegB {
		return errors.New("pair symbols must differ")
	}
	if c.Lots <= 0 {
		return errors.New("order volume must be positive")
	}
	if c.Ratio <= 0 {
		return errors.New("hedge ratio must be positive")
	}
	switch c.Mode {
	case ModeTwoLimits, ModeLimitA, ModeLimitB, ModeMarket:
	default:
		return fmt.Errorf("bad mode %d", c.Mode)
	}
	if c.Ratio > 1 && c.Mode == ModeTwoLimits {
		return fmt.Errorf("hedge ratio %d requires a single-maker or taker mode", c.Ratio)
	}
	if c.BookMaxAge < 0 || c.PriceWait < 0 || c.MinRest < 0 || c.TradeTimeout < 0 ||
		c.RetryWait < 0 || c.RetryMax < 0 || c.MarketCheckAfter < 0 ||
		c.MarketCheckEvery < 0 {
		return errors.New("durations must not be negative")
	}
	if c.HedgeTries < 1 {
		return errors.New("hedge retries must be at least one")
	}
	if c.RejectRetryLotStep < 0 || c.RejectRetryMinLots < 1 {
		return errors.New("reject retry step must be non-negative and minimum lots positive")
	}
	return nil
}

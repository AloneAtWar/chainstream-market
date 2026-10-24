package storage

// String returns the side's stable name, used by stores and the API.
func (side Side) String() string {
	switch side {
	case SideBuy:
		return "buy"
	case SideSell:
		return "sell"
	case SideLiquidityAdded:
		return "liquidity_added"
	case SideLiquidityRemoved:
		return "liquidity_removed"
	case SideUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// String returns the source's stable name.
func (source PriceSource) String() string {
	switch source {
	case PriceSourceAnchor:
		return "anchor"
	case PriceSourceGraph:
		return "graph"
	case PriceSourceUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// String returns the subject's stable name.
func (subject CandleSubject) String() string {
	switch subject {
	case CandleSubjectPool:
		return "pool"
	case CandleSubjectToken:
		return "token"
	case CandleSubjectUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// ParseSide is the inverse of Side.String.
func ParseSide(name string) Side {
	for _, side := range []Side{SideBuy, SideSell, SideLiquidityAdded, SideLiquidityRemoved} {
		if side.String() == name {
			return side
		}
	}
	return SideUnknown
}

// ParsePriceSource is the inverse of PriceSource.String.
func ParsePriceSource(name string) PriceSource {
	for _, source := range []PriceSource{PriceSourceAnchor, PriceSourceGraph} {
		if source.String() == name {
			return source
		}
	}
	return PriceSourceUnknown
}

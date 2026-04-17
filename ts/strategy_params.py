from dataclasses import dataclass, field
from typing import Dict, List, Mapping

from ts import ts_config
from ts.ts_time import SEARCH_SLOT_GROUPS


def _clone_slot_groups(src: Mapping[int, list[int]]) -> Dict[int, List[int]]:
    return {int(hour): list(slots) for hour, slots in src.items()}


@dataclass(frozen=True, slots=True)
class StrategyParams:
    # ============================================================
    # Pearson / time window
    # ============================================================
    pearson_bar_interval_seconds: int = ts_config.PEARSON_BAR_INTERVAL_SECONDS
    pearson_hour_seconds: int = ts_config.PEARSON_HOUR_SECONDS
    pearson_eval_start_minute: int = ts_config.PEARSON_EVAL_START_MINUTE
    pearson_eval_end_minute: int = ts_config.PEARSON_EVAL_END_MINUTE
    pearson_shortlist_min_correlation: float = ts_config.PEARSON_SHORTLIST_MIN_CORRELATION
    pearson_shortlist_top_n: int = ts_config.PEARSON_SHORTLIST_TOP_N
    pearson_min_shortlist: int = ts_config.PEARSON_MIN_SHORTLIST

    # ============================================================
    # Similarity weights
    # ============================================================
    similarity_weight_pearson: float = ts_config.SIMILARITY_WEIGHT_PEARSON
    similarity_weight_range: float = ts_config.SIMILARITY_WEIGHT_RANGE
    similarity_weight_net_move: float = ts_config.SIMILARITY_WEIGHT_NET_MOVE
    similarity_weight_range_position: float = ts_config.SIMILARITY_WEIGHT_RANGE_POSITION
    similarity_weight_mean_abs_diff: float = ts_config.SIMILARITY_WEIGHT_MEAN_ABS_DIFF
    similarity_weight_efficiency: float = ts_config.SIMILARITY_WEIGHT_EFFICIENCY

    # ============================================================
    # Forecast
    # ============================================================
    forecast_top_n_after_similarity: int = ts_config.FORECAST_TOP_N_AFTER_SIMILARITY

    # ============================================================
    # Decision layer
    # ============================================================
    decision_min_similarity_candidates: int = ts_config.DECISION_MIN_SIMILARITY_CANDIDATES
    decision_min_forecast_candidates: int = ts_config.DECISION_MIN_FORECAST_CANDIDATES
    decision_min_best_similarity_score: float = ts_config.DECISION_MIN_BEST_SIMILARITY_SCORE
    decision_min_last_similarity_score: float = ts_config.DECISION_MIN_LAST_SIMILARITY_SCORE
    decision_min_directional_ratio: float = ts_config.DECISION_MIN_DIRECTIONAL_RATIO
    decision_min_mean_final_move_abs: float = ts_config.DECISION_MIN_MEAN_FINAL_MOVE_ABS
    decision_min_median_final_move_abs: float = ts_config.DECISION_MIN_MEDIAN_FINAL_MOVE_ABS
    decision_require_mean_and_median_same_direction: bool = (
        ts_config.DECISION_REQUIRE_MEAN_AND_MEDIAN_SAME_DIRECTION
    )
    decision_use_adverse_move_filter: bool = ts_config.DECISION_USE_ADVERSE_MOVE_FILTER
    decision_max_mean_adverse_move_abs: float = ts_config.DECISION_MAX_MEAN_ADVERSE_MOVE_ABS

    # ============================================================
    # Hour-slot search groups
    # ============================================================
    search_slot_groups: Dict[int, List[int]] = field(
        default_factory=lambda: _clone_slot_groups(SEARCH_SLOT_GROUPS)
    )

    def pearson_bars_per_minute(self) -> int:
        return 60 // self.pearson_bar_interval_seconds

    def pearson_hour_bar_count(self) -> int:
        return self.pearson_hour_seconds // self.pearson_bar_interval_seconds

    def pearson_eval_start_bar_count(self) -> int:
        return self.pearson_eval_start_minute * self.pearson_bars_per_minute()

    def pearson_eval_end_bar_count_exclusive(self) -> int:
        return self.pearson_eval_end_minute * self.pearson_bars_per_minute()

    def pearson_has_enough_shortlist_candidates(self, candidate_count: int) -> bool:
        return candidate_count >= self.pearson_min_shortlist

    def resolve_allowed_hour_slots(self, current_hour_slot: int) -> list[int]:
        return list(self.search_slot_groups[current_hour_slot])


DEFAULT_STRATEGY_PARAMS = StrategyParams()

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
    trading_half_hour_mode: ts_config.TradingHalfHourMode = ts_config.TRADING_HALF_HOUR_MODE

    # ============================================================
    # Similarity weights
    # ============================================================
    similarity_weight_pearson: float = ts_config.SIMILARITY_WEIGHT_PEARSON
    similarity_weight_range: float = ts_config.SIMILARITY_WEIGHT_RANGE
    similarity_weight_net_move: float = ts_config.SIMILARITY_WEIGHT_NET_MOVE
    similarity_weight_range_position: float = ts_config.SIMILARITY_WEIGHT_RANGE_POSITION
    similarity_weight_mean_abs_diff: float = ts_config.SIMILARITY_WEIGHT_MEAN_ABS_DIFF
    similarity_weight_efficiency: float = ts_config.SIMILARITY_WEIGHT_EFFICIENCY
    similarity_weight_diff_pearson: float = ts_config.SIMILARITY_WEIGHT_DIFF_PEARSON
    similarity_weight_diff_sign_match: float = ts_config.SIMILARITY_WEIGHT_DIFF_SIGN_MATCH

    # ============================================================
    # Forecast
    # ============================================================
    forecast_top_n_after_similarity: int = ts_config.FORECAST_TOP_N_AFTER_SIMILARITY

    # ============================================================
    # Decision layer
    # ============================================================
    decision_min_last_similarity_score: float = ts_config.DECISION_MIN_LAST_SIMILARITY_SCORE
    decision_min_directional_ratio: float = ts_config.DECISION_MIN_DIRECTIONAL_RATIO
    decision_min_final_move_points: float = ts_config.DECISION_MIN_FINAL_MOVE_POINTS
    decision_require_mean_and_median_same_direction: bool = (
        ts_config.DECISION_REQUIRE_MEAN_AND_MEDIAN_SAME_DIRECTION
    )
    decision_use_adverse_move_filter: bool = ts_config.DECISION_USE_ADVERSE_MOVE_FILTER
    decision_max_mean_adverse_move_points: float = ts_config.DECISION_MAX_MEAN_ADVERSE_MOVE_POINTS

    # ============================================================
    # Hour-slot search groups
    # ============================================================
    search_slot_groups: Dict[int, List[int]] = field(
        default_factory=lambda: _clone_slot_groups(SEARCH_SLOT_GROUPS)
    )

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        self._validate_pearson_window_params()
        self._validate_candidate_counts()
        self._validate_trading_half_hour_mode()
        self._validate_decision_params()

    def _validate_pearson_window_params(self) -> None:
        if self.pearson_bar_interval_seconds <= 0:
            raise ValueError(
                "pearson_bar_interval_seconds must be positive: "
                f"pearson_bar_interval_seconds={self.pearson_bar_interval_seconds}"
            )

        if self.pearson_hour_seconds <= 0:
            raise ValueError(
                "pearson_hour_seconds must be positive: "
                f"pearson_hour_seconds={self.pearson_hour_seconds}"
            )

        if 60 % self.pearson_bar_interval_seconds != 0:
            raise ValueError(
                "pearson_bar_interval_seconds must divide 60 seconds exactly: "
                f"pearson_bar_interval_seconds={self.pearson_bar_interval_seconds}"
            )

        if self.pearson_hour_seconds % self.pearson_bar_interval_seconds != 0:
            raise ValueError(
                "pearson_hour_seconds must be divisible by pearson_bar_interval_seconds: "
                f"pearson_hour_seconds={self.pearson_hour_seconds}, "
                f"pearson_bar_interval_seconds={self.pearson_bar_interval_seconds}"
            )

        hour_minutes = self.pearson_hour_seconds // 60

        if not (0 <= self.pearson_eval_start_minute < hour_minutes):
            raise ValueError(
                "pearson_eval_start_minute must be inside the analysis window: "
                f"pearson_eval_start_minute={self.pearson_eval_start_minute}, "
                f"hour_minutes={hour_minutes}"
            )

        if not (0 < self.pearson_eval_end_minute <= hour_minutes):
            raise ValueError(
                "pearson_eval_end_minute must be inside the analysis window: "
                f"pearson_eval_end_minute={self.pearson_eval_end_minute}, "
                f"hour_minutes={hour_minutes}"
            )

        if self.pearson_eval_start_minute >= self.pearson_eval_end_minute:
            raise ValueError(
                "pearson_eval_start_minute must be < pearson_eval_end_minute: "
                f"pearson_eval_start_minute={self.pearson_eval_start_minute}, "
                f"pearson_eval_end_minute={self.pearson_eval_end_minute}"
            )

    def _validate_candidate_counts(self) -> None:
        if self.pearson_shortlist_top_n <= 0:
            raise ValueError(
                "pearson_shortlist_top_n must be positive: "
                f"pearson_shortlist_top_n={self.pearson_shortlist_top_n}"
            )

        if self.pearson_min_shortlist <= 0:
            raise ValueError(
                "pearson_min_shortlist must be positive: "
                f"pearson_min_shortlist={self.pearson_min_shortlist}"
            )

        if self.forecast_top_n_after_similarity <= 0:
            raise ValueError(
                "forecast_top_n_after_similarity must be positive: "
                f"forecast_top_n_after_similarity={self.forecast_top_n_after_similarity}"
            )

        if self.pearson_min_shortlist > self.pearson_shortlist_top_n:
            raise ValueError(
                "pearson_min_shortlist must be <= pearson_shortlist_top_n: "
                f"pearson_min_shortlist={self.pearson_min_shortlist}, "
                f"pearson_shortlist_top_n={self.pearson_shortlist_top_n}"
            )

        if self.forecast_top_n_after_similarity > self.pearson_shortlist_top_n:
            raise ValueError(
                "forecast_top_n_after_similarity must be <= pearson_shortlist_top_n: "
                f"forecast_top_n_after_similarity={self.forecast_top_n_after_similarity}, "
                f"pearson_shortlist_top_n={self.pearson_shortlist_top_n}"
            )

    def _validate_trading_half_hour_mode(self) -> None:
        try:
            ts_config.TradingHalfHourMode(self.trading_half_hour_mode)
        except ValueError as exc:
            raise ValueError(
                "Unsupported trading_half_hour_mode: "
                f"trading_half_hour_mode={self.trading_half_hour_mode!r}"
            ) from exc

    def _validate_decision_params(self) -> None:
        if not (-1.0 <= self.pearson_shortlist_min_correlation <= 1.0):
            raise ValueError(
                "pearson_shortlist_min_correlation must be in [-1.0, 1.0]: "
                f"pearson_shortlist_min_correlation={self.pearson_shortlist_min_correlation}"
            )

        if self.decision_min_last_similarity_score < 0.0:
            raise ValueError(
                "decision_min_last_similarity_score must be >= 0.0: "
                f"decision_min_last_similarity_score={self.decision_min_last_similarity_score}"
            )

        if not (0.0 <= self.decision_min_directional_ratio <= 1.0):
            raise ValueError(
                "decision_min_directional_ratio must be in [0.0, 1.0]: "
                f"decision_min_directional_ratio={self.decision_min_directional_ratio}"
            )

        if self.decision_min_final_move_points < 0.0:
            raise ValueError(
                "decision_min_final_move_points must be >= 0.0: "
                f"decision_min_final_move_points={self.decision_min_final_move_points}"
            )

        if self.decision_max_mean_adverse_move_points < 0.0:
            raise ValueError(
                "decision_max_mean_adverse_move_points must be >= 0.0: "
                f"decision_max_mean_adverse_move_points={self.decision_max_mean_adverse_move_points}"
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

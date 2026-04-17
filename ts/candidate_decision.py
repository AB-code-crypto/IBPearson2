from ts.strategy_params import DEFAULT_STRATEGY_PARAMS, StrategyParams


def get_move_direction(value):
    if value > 0.0:
        return "UP"
    if value < 0.0:
        return "DOWN"
    return "FLAT"


def build_no_trade_result(reason, diagnostics):
    return {
        "decision": "NO_TRADE",
        "reason": reason,
        "diagnostics": diagnostics,
    }


def build_trade_result(decision, reason, diagnostics):
    return {
        "decision": decision,
        "reason": reason,
        "diagnostics": diagnostics,
    }


def build_decision_diagnostics(ranked_similarity_candidates, forecast_summary):
    best_similarity_score = None
    last_similarity_score = None

    if ranked_similarity_candidates:
        best_similarity_score = ranked_similarity_candidates[0]["final_score"]
        last_similarity_score = ranked_similarity_candidates[-1]["final_score"]

    mean_final_move = 0.0
    median_final_move = 0.0
    positive_ratio = 0.0
    negative_ratio = 0.0
    candidate_count = 0
    mean_max_upside = 0.0
    mean_max_drawdown = 0.0

    if forecast_summary is not None:
        mean_final_move = forecast_summary["mean_final_move"]
        median_final_move = forecast_summary["median_final_move"]
        positive_ratio = forecast_summary["positive_ratio"]
        negative_ratio = forecast_summary["negative_ratio"]
        candidate_count = forecast_summary["candidate_count"]
        mean_max_upside = forecast_summary["mean_max_upside"]
        mean_max_drawdown = forecast_summary["mean_max_drawdown"]

    return {
        "similarity_candidate_count": len(ranked_similarity_candidates),
        "forecast_candidate_count": candidate_count,
        "best_similarity_score": best_similarity_score,
        "last_similarity_score": last_similarity_score,
        "mean_final_move": mean_final_move,
        "median_final_move": median_final_move,
        "mean_direction": get_move_direction(mean_final_move),
        "median_direction": get_move_direction(median_final_move),
        "positive_ratio": positive_ratio,
        "negative_ratio": negative_ratio,
        "mean_max_upside": mean_max_upside,
        "mean_max_drawdown": mean_max_drawdown,
    }


def evaluate_decision_layer(
        ranked_similarity_candidates,
        forecast_summary,
        params: StrategyParams = DEFAULT_STRATEGY_PARAMS,
):
    # Первый простой decision layer.
    #
    # На входе:
    # - ranked_similarity_candidates
    # - forecast_summary
    #
    # На выходе:
    # {
    #   "decision": "LONG" / "SHORT" / "NO_TRADE",
    #   "reason": "...",
    #   "diagnostics": {...}
    # }
    diagnostics = build_decision_diagnostics(
        ranked_similarity_candidates=ranked_similarity_candidates,
        forecast_summary=forecast_summary,
    )

    if forecast_summary is None:
        return build_no_trade_result(
            reason="forecast_summary отсутствует",
            diagnostics=diagnostics,
        )

    best_similarity_score = diagnostics["best_similarity_score"]
    if best_similarity_score is None:
        return build_no_trade_result(
            reason="Не удалось определить best_similarity_score",
            diagnostics=diagnostics,
        )

    if best_similarity_score < params.decision_min_best_similarity_score:
        return build_no_trade_result(
            reason=(
                f"Лучший similarity-score слишком мал: "
                f"{best_similarity_score:.4f} < {params.decision_min_best_similarity_score:.4f}"
            ),
            diagnostics=diagnostics,
        )

    last_similarity_score = diagnostics["last_similarity_score"]
    if last_similarity_score is None:
        return build_no_trade_result(
            reason="Не удалось определить last_similarity_score",
            diagnostics=diagnostics,
        )

    if last_similarity_score < params.decision_min_last_similarity_score:
        return build_no_trade_result(
            reason=(
                f"Последний similarity-score в top-N слишком мал: "
                f"{last_similarity_score:.4f} < {params.decision_min_last_similarity_score:.4f}"
            ),
            diagnostics=diagnostics,
        )

    mean_direction = diagnostics["mean_direction"]
    median_direction = diagnostics["median_direction"]

    if params.decision_require_mean_and_median_same_direction:
        if mean_direction != median_direction:
            return build_no_trade_result(
                reason=(
                    f"Mean и median не согласованы по направлению: "
                    f"{mean_direction} vs {median_direction}"
                ),
                diagnostics=diagnostics,
            )

        if mean_direction == "FLAT":
            return build_no_trade_result(
                reason="Mean и median не дают направленного прогноза",
                diagnostics=diagnostics,
            )

    mean_final_move_abs = abs(diagnostics["mean_final_move"])
    median_final_move_abs = abs(diagnostics["median_final_move"])

    if mean_final_move_abs < params.decision_min_mean_final_move_abs:
        return build_no_trade_result(
            reason=(
                f"Среднее ожидаемое движение слишком мало: "
                f"{mean_final_move_abs:.6f} < {params.decision_min_mean_final_move_abs:.6f}"
            ),
            diagnostics=diagnostics,
        )

    if median_final_move_abs < params.decision_min_median_final_move_abs:
        return build_no_trade_result(
            reason=(
                f"Медианное ожидаемое движение слишком мало: "
                f"{median_final_move_abs:.6f} < {params.decision_min_median_final_move_abs:.6f}"
            ),
            diagnostics=diagnostics,
        )

    if mean_direction == "UP":
        if diagnostics["positive_ratio"] < params.decision_min_directional_ratio:
            return build_no_trade_result(
                reason=(
                    f"Недостаточная доля кандидатов вверх: "
                    f"{diagnostics['positive_ratio']:.4f} < {params.decision_min_directional_ratio:.4f}"
                ),
                diagnostics=diagnostics,
            )

        if params.decision_use_adverse_move_filter:
            if abs(diagnostics["mean_max_drawdown"]) > params.decision_max_mean_adverse_move_abs:
                return build_no_trade_result(
                    reason=(
                        f"Слишком большой средний adverse move для LONG: "
                        f"{abs(diagnostics['mean_max_drawdown']):.6f} > "
                        f"{params.decision_max_mean_adverse_move_abs:.6f}"
                    ),
                    diagnostics=diagnostics,
                )

        return build_trade_result(
            decision="LONG",
            reason="Прогнозный слой согласован вверх",
            diagnostics=diagnostics,
        )

    if mean_direction == "DOWN":
        if diagnostics["negative_ratio"] < params.decision_min_directional_ratio:
            return build_no_trade_result(
                reason=(
                    f"Недостаточная доля кандидатов вниз: "
                    f"{diagnostics['negative_ratio']:.4f} < {params.decision_min_directional_ratio:.4f}"
                ),
                diagnostics=diagnostics,
            )

        if params.decision_use_adverse_move_filter:
            if diagnostics["mean_max_upside"] > params.decision_max_mean_adverse_move_abs:
                return build_no_trade_result(
                    reason=(
                        f"Слишком большой средний adverse move для SHORT: "
                        f"{diagnostics['mean_max_upside']:.6f} > "
                        f"{params.decision_max_mean_adverse_move_abs:.6f}"
                    ),
                    diagnostics=diagnostics,
                )

        return build_trade_result(
            decision="SHORT",
            reason="Прогнозный слой согласован вниз",
            diagnostics=diagnostics,
        )

    return build_no_trade_result(
        reason="Направление прогноза не определено",
        diagnostics=diagnostics,
    )

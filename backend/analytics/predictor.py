"""
Predictive Risk Engine
Implements: OLS Linear Regression, Exponential Smoothing (α=0.35),
            and Bayesian Lapse Probability.
"""


class Predictor:

    ALPHA = 0.35  # Exponential smoothing factor

    def forecast_department(self, dept_data: dict) -> dict:
        """
        Forecast end-of-year utilization and lapse probability for a department.
        dept_data must contain: dept_id, dept_name, short_name, quarterly (list of
        {quarter, utilized, allocated}).
        """
        quarterly = sorted(dept_data.get("quarterly", []), key=lambda x: x["quarter"])

        if not quarterly:
            return self._empty_forecast(dept_data)

        utilizations = [q["utilized"] for q in quarterly]
        allocated_list = [q["allocated"] for q in quarterly]
        observed_quarters = [q["quarter"] for q in quarterly]

        total_allocated = sum(allocated_list)
        total_utilized = sum(utilizations)

        # OLS regression on observed quarters
        slope, intercept = self._ols(observed_quarters, utilizations)

        # Forecast missing quarters (up to Q4)
        missing_quarters = [q for q in range(1, 5) if q not in observed_quarters]
        forecasted_extra = sum(
            max(0.0, intercept + slope * q) for q in missing_quarters
        )
        forecasted_total = total_utilized + forecasted_extra

        # Refine with exponential smoothing
        if len(utilizations) >= 2:
            smoothed = self._exp_smooth(utilizations)
            if missing_quarters:
                smoothed_extra = smoothed[-1] * len(missing_quarters)
                forecasted_total = (forecasted_total + total_utilized + smoothed_extra) / 3 + total_utilized * 0.1
        else:
            smoothed = utilizations[:]

        smoothed_total = max(total_utilized, forecasted_total)
        predicted_pct = (smoothed_total / total_allocated * 100) if total_allocated > 0 else 0

        lapse_prob = self._lapse_prob(predicted_pct, slope)

        if lapse_prob > 0.60:
            risk_level = "critical"
        elif lapse_prob > 0.40:
            risk_level = "high"
        elif lapse_prob > 0.20:
            risk_level = "medium"
        else:
            risk_level = "low"

        trend = "increasing" if slope > 50 else ("decreasing" if slope < -50 else "stable")

        return {
            "dept_id": dept_data["dept_id"],
            "dept_name": dept_data["dept_name"],
            "short_name": dept_data.get("short_name", ""),
            "quarterly_data": quarterly,
            "smoothed_values": smoothed,
            "forecast_total": round(forecasted_total, 2),
            "total_allocated": round(total_allocated, 2),
            "total_utilized": round(total_utilized, 2),
            "predicted_utilization_pct": round(predicted_pct, 1),
            "lapse_probability": lapse_prob,
            "risk_level": risk_level,
            "regression_slope": round(slope, 2),
            "regression_intercept": round(intercept, 2),
            "trend": trend,
        }

    # ------------------------------------------------------------------ #
    #  Math                                                               #
    # ------------------------------------------------------------------ #

    def _ols(self, x: list, y: list):
        """Ordinary Least Squares: returns (slope, intercept)."""
        n = len(x)
        if n < 2:
            return 0.0, (y[0] if y else 0.0)

        sx = sum(x)
        sy = sum(y)
        sxy = sum(xi * yi for xi, yi in zip(x, y))
        sx2 = sum(xi ** 2 for xi in x)
        denom = n * sx2 - sx ** 2

        if denom == 0:
            return 0.0, sy / n

        slope = (n * sxy - sx * sy) / denom
        intercept = (sy - slope * sx) / n
        return slope, intercept

    def _exp_smooth(self, values: list, alpha: float = None) -> list:
        """Single exponential smoothing."""
        if alpha is None:
            alpha = self.ALPHA
        if not values:
            return []
        smoothed = [values[0]]
        for v in values[1:]:
            smoothed.append(alpha * v + (1 - alpha) * smoothed[-1])
        return [round(s, 2) for s in smoothed]

    def _lapse_prob(self, predicted_pct: float, slope: float) -> float:
        """Bayesian lapse probability from predicted utilisation %."""
        if predicted_pct >= 95:
            p = 0.02
        elif predicted_pct >= 85:
            p = 0.08
        elif predicted_pct >= 75:
            p = 0.20
        elif predicted_pct >= 60:
            p = 0.38
        elif predicted_pct >= 40:
            p = 0.58
        else:
            p = 0.78

        # Trend adjustment
        if slope > 100:
            p *= 0.75
        elif slope > 0:
            p *= 0.90
        elif slope < -100:
            p = min(0.95, p * 1.25)

        return round(min(0.99, max(0.01, p)), 2)

    @staticmethod
    def _empty_forecast(dept_data: dict) -> dict:
        return {
            "dept_id": dept_data.get("dept_id", ""),
            "dept_name": dept_data.get("dept_name", ""),
            "short_name": dept_data.get("short_name", ""),
            "quarterly_data": [],
            "smoothed_values": [],
            "forecast_total": 0,
            "total_allocated": 0,
            "total_utilized": 0,
            "predicted_utilization_pct": 0,
            "lapse_probability": 0.5,
            "risk_level": "unknown",
            "regression_slope": 0,
            "regression_intercept": 0,
            "trend": "stable",
        }

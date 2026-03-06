"""
Anomaly Detection Engine
Implements: Z-Score, IQR, Benford's Law, Salami Slicing, Year-End Rush,
            and a 7-signal Composite Risk Score.
"""
import math
import statistics
from datetime import datetime


class AnomalyDetector:

    Z_THRESHOLD = 2.5           # Z-score flag threshold
    BENFORD_CHI_THRESHOLD = 15.0  # Chi-squared threshold
    SALAMI_RATIO = 0.0012       # 0.12% of allocation = approval threshold proxy
    YEAREND_THRESHOLD = 0.45    # >45% spending in Q4 = year-end rush

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def scan(self, transactions: list, fiscal_year: str):
        """
        Run all detection algorithms.
        Returns (anomalies_list, risk_scores_list).
        """
        by_scheme = {}
        for txn in transactions:
            sid = txn["scheme_id"]
            by_scheme.setdefault(sid, []).append(txn)

        anomalies = []
        risk_scores = []
        scan_time = datetime.now().isoformat()
        anom_idx = 0
        risk_idx = 0

        for scheme_id, txns in by_scheme.items():
            dept_id = txns[0]["dept_id"]
            total_allocation = float(txns[0].get("total_allocation", 1) or 1)
            amounts = [float(t["amount"]) for t in txns]

            # --- Signal 1: Z-Score outliers ---
            z_anoms, zscore_flag = self._zscore_detection(
                txns, amounts, scheme_id, dept_id, scan_time, anom_idx
            )
            anom_idx += len(z_anoms)
            anomalies.extend(z_anoms)

            # --- Signal 2: IQR outliers ---
            iqr_anoms, iqr_flag = self._iqr_detection(
                txns, amounts, scheme_id, dept_id, scan_time, anom_idx
            )
            anom_idx += len(iqr_anoms)
            anomalies.extend(iqr_anoms)

            # --- Signal 3: Benford's Law ---
            benford_score = self._benford_analysis(amounts)
            benford_flag = 0
            if benford_score > self.BENFORD_CHI_THRESHOLD and len(amounts) >= 5:
                benford_flag = 1
                sev = "critical" if benford_score > 28 else ("high" if benford_score > 20 else "medium")
                anomalies.append({
                    "id": f"AN{anom_idx:05d}",
                    "txn_id": None,
                    "scheme_id": scheme_id,
                    "dept_id": dept_id,
                    "anomaly_type": "benford_violation",
                    "severity": sev,
                    "score": min(100, round((benford_score / 30.0) * 100, 1)),
                    "description": (
                        f"Benford's Law violation in {scheme_id}: digit distribution chi² = {benford_score:.1f} "
                        f"(threshold {self.BENFORD_CHI_THRESHOLD}). Potential data manipulation detected."
                    ),
                    "detected_at": scan_time,
                })
                anom_idx += 1

            # --- Signal 4: Salami Slicing ---
            salami_score, salami_flag = self._detect_salami_slicing(txns, total_allocation)
            if salami_flag:
                cluster_count = sum(
                    1 for t in txns
                    if total_allocation * self.SALAMI_RATIO * 0.80 <= float(t["amount"])
                    <= total_allocation * self.SALAMI_RATIO * 0.99
                )
                anomalies.append({
                    "id": f"AN{anom_idx:05d}",
                    "txn_id": None,
                    "scheme_id": scheme_id,
                    "dept_id": dept_id,
                    "anomaly_type": "salami_slicing",
                    "severity": "high" if salami_score > 50 else "medium",
                    "score": salami_score,
                    "description": (
                        f"Salami slicing suspected in {scheme_id}: {cluster_count} transactions "
                        f"clustered just below the ₹{total_allocation * self.SALAMI_RATIO:,.0f} Cr "
                        f"approval threshold, possibly by the same vendor."
                    ),
                    "detected_at": scan_time,
                })
                anom_idx += 1

            # --- Signal 5: Year-End Rush ---
            ye_score, yearend_flag = self._detect_yearend_rush(txns, amounts)
            if yearend_flag:
                q4_total = sum(float(t["amount"]) for t in txns if self._quarter(t["txn_date"]) == 4)
                total_spend = sum(amounts) or 1
                q4_pct = q4_total / total_spend * 100
                anomalies.append({
                    "id": f"AN{anom_idx:05d}",
                    "txn_id": None,
                    "scheme_id": scheme_id,
                    "dept_id": dept_id,
                    "anomaly_type": "year_end_rush",
                    "severity": "high" if q4_pct > 58 else "medium",
                    "score": ye_score,
                    "description": (
                        f"Year-end rush in {scheme_id}: {q4_pct:.1f}% of annual budget spent in Q4 "
                        f"(safe threshold: {self.YEAREND_THRESHOLD * 100:.0f}%). Risk of poor utilization quality."
                    ),
                    "detected_at": scan_time,
                })
                anom_idx += 1

            # --- Composite Risk Score ---
            composite = self._composite_score(
                zscore_flag, iqr_flag, benford_score, salami_score,
                ye_score, amounts, total_allocation,
            )
            lapse_prob = self._lapse_probability(txns, amounts, total_allocation)

            risk_scores.append({
                "id": f"RS{risk_idx:05d}",
                "dept_id": dept_id,
                "scheme_id": scheme_id,
                "composite_score": composite,
                "zscore_flag": zscore_flag,
                "benford_flag": benford_flag,
                "salami_flag": int(salami_flag),
                "yearend_flag": int(yearend_flag),
                "lapse_probability": lapse_prob,
                "calculated_at": scan_time,
            })
            risk_idx += 1

        return anomalies, risk_scores

    # ------------------------------------------------------------------ #
    #  Detection Algorithms                                               #
    # ------------------------------------------------------------------ #

    def _zscore_detection(self, txns, amounts, scheme_id, dept_id, scan_time, start_idx):
        anoms = []
        flag = 0
        if len(amounts) < 3:
            return anoms, flag

        mean = statistics.mean(amounts)
        std = statistics.stdev(amounts) if len(amounts) > 1 else 0
        if std == 0:
            return anoms, flag

        idx = start_idx
        for txn in txns:
            z = abs((float(txn["amount"]) - mean) / std)
            if z >= self.Z_THRESHOLD:
                flag = 1
                sev = "critical" if z >= 4.5 else ("high" if z >= 3.5 else "medium")
                anoms.append({
                    "id": f"AN{idx:05d}",
                    "txn_id": txn["id"],
                    "scheme_id": scheme_id,
                    "dept_id": dept_id,
                    "anomaly_type": "z_score_outlier",
                    "severity": sev,
                    "score": min(100, round((z / 5.0) * 100, 1)),
                    "description": (
                        f"Transaction {txn['id']} (₹{float(txn['amount']):,.0f} Cr) is {z:.2f}σ "
                        f"above the scheme mean of ₹{mean:,.0f} Cr. Vendor: {txn.get('vendor', 'N/A')}."
                    ),
                    "detected_at": scan_time,
                })
                idx += 1

        return anoms, flag

    def _iqr_detection(self, txns, amounts, scheme_id, dept_id, scan_time, start_idx):
        anoms = []
        flag = 0
        if len(amounts) < 4:
            return anoms, flag

        sorted_a = sorted(amounts)
        n = len(sorted_a)
        q1 = sorted_a[n // 4]
        q3 = sorted_a[3 * n // 4]
        iqr = q3 - q1
        if iqr == 0:
            return anoms, flag

        upper_fence = q3 + 2.5 * iqr
        idx = start_idx
        for txn in txns:
            if float(txn["amount"]) > upper_fence:
                flag = 1
                ratio = float(txn["amount"]) / upper_fence
                anoms.append({
                    "id": f"AN{idx:05d}",
                    "txn_id": txn["id"],
                    "scheme_id": scheme_id,
                    "dept_id": dept_id,
                    "anomaly_type": "iqr_outlier",
                    "severity": "high" if ratio > 2.0 else "medium",
                    "score": min(100, round((ratio - 1) / 2 * 100, 1)),
                    "description": (
                        f"IQR outlier: {txn['id']} (₹{float(txn['amount']):,.0f} Cr) exceeds "
                        f"the upper fence of ₹{upper_fence:,.0f} Cr by {(ratio - 1) * 100:.0f}%."
                    ),
                    "detected_at": scan_time,
                })
                idx += 1

        return anoms, flag

    def _benford_analysis(self, amounts: list) -> float:
        """Return chi-squared statistic against Benford's first-digit law."""
        expected = {1: 0.301, 2: 0.176, 3: 0.125, 4: 0.097,
                    5: 0.079, 6: 0.067, 7: 0.058, 8: 0.051, 9: 0.046}
        counts = {d: 0 for d in range(1, 10)}
        total = 0
        for amt in amounts:
            if amt <= 0:
                continue
            digits = str(abs(amt)).replace(".", "").lstrip("0")
            if not digits:
                continue
            d = int(digits[0])
            if 1 <= d <= 9:
                counts[d] += 1
                total += 1

        if total < 5:
            return 0.0

        chi_sq = sum(
            (counts[d] - total * expected[d]) ** 2 / (total * expected[d])
            for d in range(1, 10)
        )
        return round(chi_sq, 2)

    def _detect_salami_slicing(self, txns, total_allocation):
        threshold = total_allocation * self.SALAMI_RATIO
        lo, hi = threshold * 0.80, threshold * 0.99
        clustered = [t for t in txns if lo <= float(t["amount"]) <= hi]
        if len(clustered) < 3:
            return 0.0, False

        vendors = [t.get("vendor", "") for t in clustered]
        unique_v = len(set(vendors))
        vendor_factor = 1.6 if unique_v == 1 else 1.2 if unique_v <= 2 else 1.0
        score = min(100.0, round(len(clustered) * 10 * vendor_factor, 1))
        return score, True

    def _detect_yearend_rush(self, txns, amounts):
        q4_total = sum(float(t["amount"]) for t in txns if self._quarter(t["txn_date"]) == 4)
        total = sum(amounts)
        if total == 0:
            return 0.0, False
        ratio = q4_total / total
        if ratio <= self.YEAREND_THRESHOLD:
            return 0.0, False
        score = min(100.0, round((ratio - self.YEAREND_THRESHOLD) / 0.35 * 100, 1))
        return score, True

    # ------------------------------------------------------------------ #
    #  Scoring Helpers                                                    #
    # ------------------------------------------------------------------ #

    def _composite_score(self, zscore_flag, iqr_flag, benford_chi, salami_score,
                         ye_score, amounts, total_allocation):
        """7-signal weighted composite risk score (0-100)."""
        score = 0.0
        # Signal 1: Z-score (weight 20)
        if zscore_flag:
            score += 20
        # Signal 2: IQR (weight 10)
        if iqr_flag:
            score += 10
        # Signal 3: Benford's (weight 22)
        if benford_chi > self.BENFORD_CHI_THRESHOLD:
            score += min(22, (benford_chi / 30.0) * 22)
        # Signal 4: Salami (weight 18)
        score += salami_score * 0.18
        # Signal 5: Year-end (weight 15)
        score += ye_score * 0.15
        # Signal 6: Utilization variance (weight 10)
        if len(amounts) > 1:
            cv = statistics.stdev(amounts) / (statistics.mean(amounts) + 1)
            score += min(10, cv * 10)
        # Signal 7: Concentration (single large txn, weight 5)
        if amounts and max(amounts) / (sum(amounts) + 1) > 0.5:
            score += 5

        return round(min(100.0, score), 1)

    def _lapse_probability(self, txns, amounts, total_allocation):
        """Bayesian-inspired lapse probability estimate."""
        utilization = sum(amounts) / (total_allocation or 1)
        if utilization >= 0.95:
            base = 0.02
        elif utilization >= 0.80:
            base = 0.10
        elif utilization >= 0.65:
            base = 0.22
        elif utilization >= 0.45:
            base = 0.45
        else:
            base = 0.72

        # Adjust for Q4 acceleration
        q4 = sum(float(t["amount"]) for t in txns if self._quarter(t["txn_date"]) == 4)
        if (sum(amounts) or 1) > 0 and q4 / sum(amounts) > 0.45:
            base = max(0.02, base - 0.08)

        return round(min(0.99, max(0.01, base)), 2)

    @staticmethod
    def _quarter(date_str: str) -> int:
        """Return fiscal quarter (April = Q1) from YYYY-MM-DD string."""
        try:
            month = int(date_str[5:7])
            return ((month - 4) % 12) // 3 + 1
        except Exception:
            return 1

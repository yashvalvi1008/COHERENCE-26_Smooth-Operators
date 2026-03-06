"""
Reallocation Optimizer
Greedy algorithm that pairs under-utilized (surplus) schemes with
over-utilized (deficit) schemes, subject to legal compatibility rules.
"""


class Optimizer:

    SURPLUS_THRESHOLD = 0.35    # util < 35%  → surplus candidate
    DEFICIT_THRESHOLD = 0.82    # util > 82%  → deficit candidate
    MAX_TRANSFER_RATIO = 0.20   # max 20% of surplus can be reallocated
    MIN_TRANSFER_AMOUNT = 100   # minimum 100 Cr transfer

    # Category cross-transfer compatibility matrix
    _COMPATIBLE = {
        "healthcare":     {"social"},
        "education":      {"social", "technology"},
        "rural":          {"agriculture", "water"},
        "agriculture":    {"rural", "water"},
        "infrastructure": {"urban"},
        "urban":          {"infrastructure", "technology"},
        "water":          {"rural", "agriculture"},
        "social":         {"healthcare", "education"},
        "technology":     {"education", "urban"},
        "finance":        set(),
    }

    def generate_recommendations(self, schemes: list) -> list:
        """
        Greedy surplus→deficit matching.
        Returns list of recommendation dicts.
        """
        surplus, deficit = [], []

        for s in schemes:
            alloc = s.get("allocated") or 0
            util  = s.get("utilized")  or 0
            if alloc == 0:
                continue
            rate = util / alloc

            if rate < self.SURPLUS_THRESHOLD:
                avail = (alloc - util) * self.MAX_TRANSFER_RATIO
                if avail >= self.MIN_TRANSFER_AMOUNT:
                    surplus.append({**s, "util_rate": rate, "surplus_available": round(avail, 2)})

            elif rate > self.DEFICIT_THRESHOLD:
                needed = util * 0.15  # estimate ~15% additional needed
                deficit.append({**s, "util_rate": rate, "deficit_needed": round(needed, 2)})

        surplus.sort(key=lambda x: x["surplus_available"], reverse=True)
        deficit.sort(key=lambda x: x["util_rate"], reverse=True)

        recommendations = []
        rec_idx = 0
        d_ptr = 0

        for sup in surplus:
            if d_ptr >= len(deficit):
                break
            while d_ptr < len(deficit) and sup["surplus_available"] >= self.MIN_TRANSFER_AMOUNT:
                dfct = deficit[d_ptr]
                if self._compatible(sup, dfct):
                    transfer = min(sup["surplus_available"], dfct["deficit_needed"])
                    if transfer >= self.MIN_TRANSFER_AMOUNT:
                        recommendations.append({
                            "id": f"REC{rec_idx:04d}",
                            "from_scheme_id":   sup["id"],
                            "from_scheme_name": sup["name"],
                            "from_dept_id":     sup.get("dept_id", ""),
                            "from_dept_name":   sup.get("dept_name", ""),
                            "from_util_pct":    round(sup["util_rate"] * 100, 1),
                            "to_scheme_id":     dfct["id"],
                            "to_scheme_name":   dfct["name"],
                            "to_dept_id":       dfct.get("dept_id", ""),
                            "to_dept_name":     dfct.get("dept_name", ""),
                            "to_util_pct":      round(dfct["util_rate"] * 100, 1),
                            "transfer_amount":  round(transfer, 2),
                            "reason":           self._reason(sup, dfct, transfer),
                            "priority":         "high" if dfct["util_rate"] > 0.94 else "medium",
                            "same_category":    sup.get("category") == dfct.get("category"),
                            "status":           "pending",
                        })
                        sup["surplus_available"]  -= transfer
                        dfct["deficit_needed"]    -= transfer
                        rec_idx += 1
                    if dfct["deficit_needed"] <= 0:
                        d_ptr += 1
                else:
                    d_ptr += 1

        return recommendations

    def _compatible(self, surplus: dict, deficit: dict) -> bool:
        sc = surplus.get("category", "")
        dc = deficit.get("category", "")
        if sc == dc:
            return True
        return dc in self._COMPATIBLE.get(sc, set())

    @staticmethod
    def _reason(sup: dict, dfct: dict, amount: float) -> str:
        return (
            f"{sup['name']} is only {sup['util_rate']*100:.1f}% utilized with "
            f"₹{amount:,.0f} Cr available for reallocation. "
            f"{dfct['name']} is running at {dfct['util_rate']*100:.1f}% and requires "
            f"additional funding to meet fiscal year targets before March 31st."
        )

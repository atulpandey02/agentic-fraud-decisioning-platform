# =============================================================
# CHART RENDERER — result shape + hint -> a Plotly figure
# =============================================================
# The LLM supplies a chart HINT (it saw the question and knows
# the intent); this class supplies the VETO (it sees the actual
# result shape and knows what's renderable). Split that way
# because each party is authoritative about exactly one half:
# the model can't know the query returns one row until it runs,
# and code can't know the user meant "trend" rather than
# "comparison". Hint proposes, shape disposes — a bar hint on a
# single-cell result still renders as a metric, and anything
# unrenderable falls back to a table rather than erroring.
# =============================================================

import logging
from typing import List, Optional

import pandas as pd
import plotly.express as px

logger = logging.getLogger(__name__)


class ChartRenderer:
    """
    Stateless: build_dataframe() turns cursor output into a
    DataFrame; render() turns DataFrame + hint into a Plotly
    figure or None (None = caller shows the plain table, which
    Streamlit does natively — no figure is better than a wrong one).
    """

    @staticmethod
    def build_dataframe(columns: List[str], rows: List[tuple]) -> pd.DataFrame:
        """
        Manual DataFrame construction instead of the connector's
        fetch_pandas_all(): that method routes through the
        connector's own pyarrow integration, which pins pyarrow
        version ranges (our pyarrow is pinned by Phase 1's Parquet
        writer — two masters, one pin). Building from plain rows
        has no such coupling and these results are ≤ BI_MAX_ROWS
        anyway, so the arrow fast path buys nothing here.
        """
        return pd.DataFrame(rows, columns=columns)

    # ----------------------------------------------------------
    def render(self, df: pd.DataFrame, chart_hint: str):
        """Return a Plotly figure, or None when a table/metric is
        the honest rendering. Never raises on shape mismatch —
        falls back instead (a dashboard that errors on odd shapes
        trains analysts to stop asking odd questions)."""
        if df.empty:
            return None

        hint = (chart_hint or "table").lower()

        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        other_cols = [c for c in df.columns if c not in numeric_cols]

        # single cell -> metric handled by the app; single row or
        # no numeric column -> table. The renderer refuses rather
        # than contorts.
        if hint in ("table", "metric") or not numeric_cols or len(df) == 1:
            return None

        x = other_cols[0] if other_cols else df.columns[0]
        y = numeric_cols[0]

        try:
            if hint == "line":
                return px.line(df, x=x, y=y, markers=True)
            if hint == "pie":
                return px.pie(df, names=x, values=y)
            # bar is both the hint default and the fallback for
            # unknown hints — the least-wrong chart for "categories
            # with numbers", which is what most audit questions are.
            return px.bar(df, x=x, y=y)
        except Exception as e:
            logger.warning(f"Chart render failed ({hint}: {e}) — falling back to table")
            return None

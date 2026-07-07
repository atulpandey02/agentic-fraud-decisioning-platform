# =============================================================
# UNIT TESTS — chart selection (Priority 4)
# =============================================================
# ChartRenderer is pure (pandas + plotly, no infra). The contract:
# the LLM's chart_hint PROPOSES, the actual data SHAPE disposes —
# and it must never raise on an odd shape (falls back to None ->
# the app shows a table).
# =============================================================

import pandas as pd

from fraud_platform.bi_dashboard.chart_renderer import ChartRenderer


R = ChartRenderer()


class TestBuildDataframe:
    def test_builds_from_rows(self):
        df = R.build_dataframe(["a", "b"], [(1, 2), (3, 4)])
        assert list(df.columns) == ["a", "b"]
        assert df.shape == (2, 2)


class TestRenderShapeDisposes:
    def test_empty_returns_none(self):
        assert R.render(pd.DataFrame(), "bar") is None

    def test_table_hint_returns_none(self):
        df = pd.DataFrame({"k": ["a", "b"], "v": [1, 2]})
        assert R.render(df, "table") is None

    def test_metric_hint_returns_none(self):
        df = pd.DataFrame({"n": [5]})
        assert R.render(df, "metric") is None

    def test_single_row_returns_none_even_with_bar_hint(self):
        # shape veto: one row is a metric, not a bar
        df = pd.DataFrame({"k": ["a"], "v": [5]})
        assert R.render(df, "bar") is None

    def test_no_numeric_column_returns_none(self):
        df = pd.DataFrame({"a": ["x", "y"], "b": ["p", "q"]})
        assert R.render(df, "bar") is None

    def test_bar_hint_renders_figure(self):
        df = pd.DataFrame({"decision": ["ALLOW", "BLOCK"], "n": [5, 3]})
        fig = R.render(df, "bar")
        assert fig is not None

    def test_line_hint_renders_figure(self):
        df = pd.DataFrame({"day": ["mon", "tue", "wed"], "n": [1, 2, 3]})
        assert R.render(df, "line") is not None

    def test_pie_hint_renders_figure(self):
        df = pd.DataFrame({"tier": ["A", "B"], "n": [7, 3]})
        assert R.render(df, "pie") is not None

    def test_unknown_hint_falls_back_to_bar(self):
        df = pd.DataFrame({"k": ["a", "b"], "v": [1, 2]})
        assert R.render(df, "sunburst-3d") is not None  # bar fallback, not a crash

    def test_never_raises_on_odd_shape(self):
        # many text cols, one numeric, weird hint — must not raise
        df = pd.DataFrame({"a": ["x", "y"], "b": ["p", "q"], "n": [1, 2]})
        R.render(df, "pie")  # no exception == pass

"""Chart query contexts and native dashboard layout; no Superset runtime imports."""


def chart_payload(spec, dataset_id):
    params = dict(
        datasource=f"{dataset_id}__table",
        viz_type=spec["viz"],
        time_range="No filter",
        granularity_sqla="order_date",
        adhoc_filters=[],
        row_limit=10000,
        color_scheme="supersetColors",
        y_axis_format=",.0f",
        show_legend=True,
    )
    query = dict(
        time_range="No filter",
        granularity="order_date",
        filters=[],
        extras={"having": "", "where": ""},
        row_limit=10000,
    )
    if "metric" in spec:
        params.update(metric=spec["metric"], y_axis_format=spec["value_format"], subheader="", header_font_size=0.4)
        query.update(metrics=[spec["metric"]], columns=[], is_timeseries=False)
    else:
        params.update(metrics=spec["metrics"], groupby=[], show_value=False)
        query.update(metrics=spec["metrics"])
        if spec.get("groupby"):
            column = spec["groupby"][0]
            params.update(
                x_axis=column,
                orientation="horizontal",
                time_grain_sqla=None,
                row_limit=10,
                sort_series_type="sum",
                sort_series_ascending=False,
            )
            query.update(columns=[column], is_timeseries=False, row_limit=10, orderby=[[spec["metrics"][0], False]])
        else:
            params.update(x_axis="order_date", time_grain_sqla="P1D", rich_tooltip=True, markerEnabled=False)
            query.update(
                columns=[
                    {
                        "columnType": "BASE_AXIS",
                        "sqlExpression": "order_date",
                        "label": "order_date",
                        "expressionType": "SQL",
                        "timeGrain": "P1D",
                    }
                ],
                granularity="order_date",
                is_timeseries=False,
                orderby=[["order_date", True]],
                time_grain_sqla="P1D",
            )
    context = dict(
        datasource={"id": dataset_id, "type": "table"},
        force=False,
        queries=[query],
        form_data=params,
        result_format="json",
        result_type="full",
    )
    return params, context


def layout(charts, publication):
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": "Doanh thu bán lẻ"}},
    }
    position["GRID_ID"]["children"].append("ROW-SCOPE")
    position["ROW-SCOPE"] = dict(
        id="ROW-SCOPE",
        type="ROW",
        children=["MARKDOWN-SCOPE"],
        parents=["ROOT_ID", "GRID_ID"],
        meta={"background": "BACKGROUND_TRANSPARENT"},
    )
    position["MARKDOWN-SCOPE"] = dict(
        id="MARKDOWN-SCOPE",
        type="MARKDOWN",
        children=[],
        parents=["ROOT_ID", "GRID_ID", "ROW-SCOPE"],
        meta={
            "code": f"Publication: `{publication.run_id}` · Công bố (UTC): {publication.published_at} · Đơn vị: VND",
            "width": 12,
            "height": 8,
        },
    )
    rows = [(charts[:4], 16), (charts[4:5], 40), (charts[5:7], 35), (charts[7:], 35)]
    for row_index, (items, height) in enumerate(rows):
        row_id = f"ROW-{row_index}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = dict(
            id=row_id,
            type="ROW",
            children=[],
            parents=["ROOT_ID", "GRID_ID"],
            meta={"background": "BACKGROUND_TRANSPARENT"},
        )
        for chart in items:
            chart_id = f"CHART-{chart.id}"
            position[row_id]["children"].append(chart_id)
            position[chart_id] = dict(
                id=chart_id,
                type="CHART",
                children=[],
                parents=["ROOT_ID", "GRID_ID", row_id],
                meta=dict(
                    chartId=chart.id,
                    sliceName=chart.slice_name,
                    uuid=str(chart.uuid),
                    width=12 // len(items),
                    height=height,
                ),
            )
    return position

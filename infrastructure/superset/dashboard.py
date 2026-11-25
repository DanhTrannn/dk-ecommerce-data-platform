"""Chart query contexts and native dashboard layout; no Superset runtime imports."""

import json


def chart_payload(spec, dataset_id):
    date_column = spec["date_column"]
    row_limit = spec.get("row_limit", 10 if spec.get("groupby") else 10000)
    filters = spec.get("filters", [])
    metrics = [spec["metric"]] if "metric" in spec else spec.get("metrics", [])
    money_axis = spec["viz"].startswith("echarts_timeseries_") and all(name.endswith("_vnd") for name in metrics)
    params = dict(
        datasource=f"{dataset_id}__table",
        viz_type=spec["viz"],
        time_range="No filter",
        granularity_sqla=date_column,
        adhoc_filters=[
            dict(clause="WHERE", expressionType="SIMPLE", subject=f["col"], operator=f["op"], comparator=f["val"])
            for f in filters
        ],
        row_limit=row_limit,
        color_scheme="supersetColors",
        y_axis_format=spec.get("value_format", ".3s" if money_axis else ",.0f"),
        show_legend=True,
    )
    query = dict(
        time_range="No filter",
        granularity=date_column,
        filters=filters,
        extras={"having": "", "where": ""},
        row_limit=row_limit,
    )
    if spec["viz"] == "table":
        columns = spec["columns"]
        order = [[column, True] for column in spec["sort_by"]]
        params.update(query_mode="raw", all_columns=columns, order_by_cols=[json.dumps(item) for item in order], page_length=10)
        query.update(columns=columns, orderby=order, is_timeseries=False)
    elif spec["viz"] == "heatmap_v2":
        x, y, metric = spec["x_axis"], spec["y_axis"], spec["metric"]
        params.update(
            x_axis=x, groupby=y, metric=metric, linear_color_scheme="blue_white_yellow",
            sort_x_axis="alpha_asc", sort_y_axis="alpha_asc", x_axis_time_format="%Y-%m",
            show_values=False,
        )
        query.update(columns=[x, y], metrics=[metric], orderby=[[x, True], [y, True]], is_timeseries=False)
    elif spec["viz"] == "waterfall":
        axis, metric = spec["x_axis"], spec["metric"]
        params.update(
            x_axis=axis, metric=metric, groupby=[], show_total=True, total_label="Lợi nhuận gộp",
            increase_label="Tăng", decrease_label="Giảm",
        )
        query.update(columns=[axis], metrics=[metric], orderby=[[axis, True]], is_timeseries=False)
    elif "metric" in spec:
        params.update(metric=spec["metric"], subheader="", header_font_size=0.4)
        query.update(metrics=[spec["metric"]], columns=[], is_timeseries=False)
    else:
        params.update(metrics=spec["metrics"], groupby=[], show_value=False)
        if spec.get("stack"):
            params["stack"] = "Stack"
        query.update(metrics=spec["metrics"])
        if spec.get("groupby"):
            column = spec["groupby"][0]
            metric = spec["metrics"][0]
            params.update(
                x_axis=column,
                orientation="horizontal",
                time_grain_sqla=None,
                sort_series_type="sum",
                sort_series_ascending=False,
            )
            if len(spec["metrics"]) == 1:
                params.update(x_axis_sort=metric, x_axis_sort_asc=False)
            query.update(columns=[column], is_timeseries=False, orderby=[[metric, False]])
        else:
            params.update(x_axis=date_column, time_grain_sqla="P1D", rich_tooltip=True, markerEnabled=False)
            query.update(
                columns=[
                    {
                        "columnType": "BASE_AXIS",
                        "sqlExpression": date_column,
                        "label": date_column,
                        "expressionType": "SQL",
                        "timeGrain": "P1D",
                    }
                ],
                granularity=date_column,
                is_timeseries=False,
                orderby=[[date_column, True]],
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


def layout(
    charts, *, title="Doanh thu bán lẻ",
    description="Đơn đặt theo ngày đặt. Doanh thu theo ngày ghi nhận. Đơn vị tiền tệ là VND.",
    rows=None,
):
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
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
            "code": description,
            "width": 12,
            "height": 8,
        },
    )
    rows = rows if rows is not None else (charts[:4], charts[4:5], charts[5:7], charts[7:])
    offset = 0
    for row_index, row in enumerate(rows):
        if not row:
            continue
        items = charts[offset:offset + len(row)]
        offset += len(row)
        height = 16 if all(chart.viz_type == "big_number_total" for chart in items) else 40
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

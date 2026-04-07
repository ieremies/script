import marimo

__generated_with = "0.22.4"
app = marimo.App(width="full")

with app.setup(hide_code=True):
    from pathlib import Path

    import altair as alt
    import marimo as mo
    import numpy as np
    import polars as pl

    from functools import reduce
    import operator

    PROJECT_ROOT = Path("/Users/ieremies/proj/color")

    # alt.data_transformers.enable("vegafusion")


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Input
    """)
    return


@app.cell(hide_code=True)
def _():
    _logs_root = PROJECT_ROOT / "logs"
    _lit_root = PROJECT_ROOT / "inst/lit"
    _logs_dir = (
        mo.watch.directory(_logs_root) if _logs_root.is_dir() else _logs_root
    )
    _lit_dir = mo.watch.directory(_lit_root) if _lit_root.is_dir() else _lit_root
    _csv_paths = get_csvs(_logs_dir)  # + get_csvs(_lit_dir)
    csvs_files = {
        f"/{n.relative_to(PROJECT_ROOT).as_posix()}": n for n in _csv_paths
    }

    files = mo.ui.multiselect(
        csvs_files,
        label="Select CSV files to plot",
        value=["/logs/held.csv", "/logs/ordering.csv"],
        full_width=True,
    )

    files
    return csvs_files, files


@app.cell
def _(switch_dict):
    switch_dict
    return


@app.cell(hide_code=True)
def _(csvs_files, files, filter_instances):
    mo.stop(csvs_files is None or len(files.value) == 0, "No files selected.")

    df = concat_dfs([get_df(f) for f in files.value], filter_instances)
    # df = df.filter(pl.col("time") > 1)

    # list of unique values in the column "instance"
    unique_instances = df["instance"].unique().to_list()

    mo.vstack(
        [
            f"Looking at {len(files.value)} results, with {len(unique_instances)} instances in common.",
            df,
        ]
    )
    return (df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Graphing
    """)
    return


@app.cell(hide_code=True)
def _(df):
    get_stats(df)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Accumulated Distribution
    """)
    return


@app.cell(hide_code=True)
def _(df):
    altair_accu(df, x_axis="time", max_x=3600.0).interactive().properties(
        title="Cumulative Time to Solve"
    ) | altair_accu(df, x_axis="gap", max_x=100).interactive().properties(
        title="Cumulative Gap"
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Performance Profile
    """)
    return


@app.cell(hide_code=True)
def _(df):
    altair_accu(
        compute_ratio(df), x_axis="ratio", max_x=1000
    ).interactive().properties(
        title="Performance Profile of the Time to Solve"
    ) | altair_accu(
        compute_ratio(df, "gap"), x_axis="ratio", max_x=10
    ).interactive().properties(title="Performance Profile of the Gap")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Where we die...
    """)
    return


@app.cell(hide_code=True)
def _(df):
    _not_solved = df.filter(pl.col("time").is_null())

    (
        _not_solved.group_by("source").agg(
            (pl.col("time").is_null()).sum().alias("total not solved"),
            (pl.col("root_lb").is_not_null()).sum().alias("solved root"),
            (pl.col("count_branch").is_null() & pl.col("root_lb").is_null())
            .sum()
            .alias("died on root"),
            (
                (pl.col("count_branch").is_null())
                & (pl.col("count_strong_branching") > 1)
            )
            .sum()
            .alias("died on strong"),
            (pl.col("count_branch") > 1).sum().alias("died branching"),
        )
    )
    return


@app.cell(hide_code=True)
def _(df):
    _fixed_root_time = df.with_columns(
        pl.when(pl.col("root_time").is_null() & pl.col("time").is_not_null())
        .then(pl.col("time"))
        .otherwise(pl.col("root_time"))
        .alias("root_time")
    )

    altair_accu(
        compute_ratio(
            _fixed_root_time.filter(pl.col("source") != "ordering"), "root_time"
        ),
        x_axis="ratio",
        max_x=2000,
    ).interactive().properties(
        title="Performance Profile of the Time to Solve the Root"
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Histogram
    """)
    return


@app.cell(hide_code=True)
def _(df):
    _sources = df["source"].unique().to_list()
    _default = "held" if "held" in _sources else _sources[0]
    base_histogram = mo.ui.radio(
        _sources, label="Which source to use as baseline?", value=_default
    )
    base_histogram
    return (base_histogram,)


@app.cell(hide_code=True)
def _(base_histogram, df):
    chart_histo = mo.ui.altair_chart(
        alt_cumulative_relative_histogram(df, base_histogram.value)
    )
    return (chart_histo,)


@app.cell
def _(chart_histo):
    mo.vstack([chart_histo, chart_histo.value])
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Outliners
    """)
    return


@app.cell(hide_code=True)
def _(df):
    _sources = df["source"].unique().to_list()
    mo.stop(len(_sources) < 2)
    _default1 = "held" if "held" in _sources else _sources[0]
    _default2 = (
        _sources[0]
        if "held" in _sources and _sources[0] != "held" in _sources
        else _sources[1]
    )

    out_source1 = mo.ui.dropdown(options=_sources, value=_default1)
    out_source2 = mo.ui.dropdown(options=_sources, value=_default2)

    time_cutoff = mo.ui.slider(
        steps=np.logspace(start=1, stop=6, base=2, dtype="int"),
        value=10,
        show_value=True,
        label="Time cutoff:",
    )

    mo.hstack(
        [
            mo.hstack(
                [
                    "Select two to compare: --|",
                    out_source1,
                    "|--  X  --|",
                    out_source2,
                ],
                justify="start",
                gap="2rem",
            ),
            time_cutoff,
        ]
    )
    return out_source1, out_source2, time_cutoff


@app.cell(hide_code=True)
def _(df, out_source1, out_source2, time_cutoff):
    mo.stop(out_source1.value == out_source2.value, "Comparing a soure to itself.")


    def _aux1(str1, str2):
        return mo.vstack(
            [
                mo.md(f"Solved by **{str1}**, but not by **{str2}**."),
                get_exclusive_solved(df, str1, str2),
            ]
        )


    def _aux2(str1, str2):
        return mo.vstack(
            [
                mo.md(
                    f"Solved by **{str1}** {time_cutoff.value} times faster than **{str2}**."
                ),
                get_solved_with_time_factor(df, str1, str2, time_cutoff.value),
            ]
        )


    mo.vstack(
        [
            mo.hstack(
                [
                    _aux1(out_source1.value, out_source2.value),
                    _aux1(out_source2.value, out_source1.value),
                ]
            ),
            mo.hstack(
                [
                    _aux2(out_source1.value, out_source2.value),
                    _aux2(out_source2.value, out_source1.value),
                ]
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(df):
    _instances = sorted(df["instance"].unique().to_list())

    instance_detail = mo.ui.dropdown(
        options=_instances,
        value=_instances[0],
        label="Select an instance to look closer: ",
    )
    instance_detail
    return (instance_detail,)


@app.cell(hide_code=True)
def _(df, instance_detail, out_source1, out_source2):
    _aux = compare_instance(
        df, instance_detail.value, out_source1.value, out_source2.value
    )

    mo.vstack([mo.hstack(l, align="start") for l in _aux])
    mo.ui.table(_aux, pagination=False, selection=None)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Histogram
    """)
    return


@app.cell(hide_code=True)
def _(df):
    # select one source to analyze (by default, one that starts with "primal_")
    _all_sources = df.select(pl.col("source").unique()).to_series().to_list()
    _default_source = [s for s in _all_sources if s.startswith("primal_")][0]

    source_histogram = mo.ui.radio(
        _all_sources,
        label="Select source to analyze:",
        value=_default_source,
    )

    histo_time_cutoff = mo.ui.slider(
        # num=6 ensures we only get 0.01, 0.1, 1, 10, 100, 1000
        steps=np.logspace(start=-2, stop=3, num=6, base=10),
        value=0.1,
        show_value=True,
        label="Time cutoff for histogram:",
    )

    show_not_solved_switch = mo.ui.switch(label="Allow instances not solved")

    mo.hstack([source_histogram, histo_time_cutoff, show_not_solved_switch])
    return histo_time_cutoff, show_not_solved_switch, source_histogram


@app.cell(hide_code=True)
def _(df, histo_time_cutoff, show_not_solved_switch, source_histogram):
    time_condition = pl.col("time") > histo_time_cutoff.value

    if show_not_solved_switch.value:
        # If True, allow BOTH times over the cutoff AND null times
        time_condition = time_condition | pl.col("time").is_null()

    # 2. Apply the filter
    _df = df.filter(
        (pl.col("source").eq(source_histogram.value))
        & ((pl.col("count_Expand").le(1)) | (pl.col("count_Expand").is_null()))
        & time_condition
    ).unique()

    _df = _df.with_columns(pl.col("time").fill_null(3600.0))

    # if the number of instances is greater than 1000, sample 1000 instances randomly
    if len(_df) > 1000:
        _df = _df.sample(n=1000, seed=42)
        print(f"Analyzing {len(_df)} instances after filtering and sampling.")
    else:
        print(f"Analyzing {len(_df)} instances.")
    # ---------------------------------------------------------
    # 2. Dynamically extract function names

    # Extract the function names by stripping out the "time_" prefix
    # Find all columns starting with "time_"
    time_cols = [col for col in _df.columns if col.startswith("time_")]
    all_functions = [col.replace("time_", "") for col in time_cols]

    valid_functions = []
    for func in all_functions:
        # if the column "time_{func}" is str, transform it to float
        if _df[f"time_{func}"].dtype == pl.Utf8:
            _df = _df.with_columns(
                pl.col(f"time_{func}").str.replace(",", ".").cast(pl.Float64)
            )
        # Calculate the average percentage of total time across all instances
        avg_pct = _df.select(
            (pl.col(f"time_{func}") / pl.col("time")).mean()
        ).item()

        # Keep only functions between 1% (0.01) and 99% (0.99)
        if avg_pct and 0.01 <= avg_pct <= 0.99:
            valid_functions.append(func)

    # ---------------------------------------------------------
    # 3. Calculate Percentages and Prepare Data
    # ---------------------------------------------------------
    # Use ONLY the valid functions moving forward
    pct_exprs = [
        (pl.col(f"time_{func}") / pl.col("time")).alias(func)
        for func in valid_functions
    ]

    # Create the Time Long dataframe
    # Optimization: Select ONLY the columns needed for the chart immediately
    df_time_long = (
        _df.select([pl.col("instance")] + pct_exprs)
        .unpivot(
            index="instance",
            variable_name="function",
            value_name="time_percentage",
        )
        # Drop 'instance' if you don't use it for tooltips or selection to save space
        .select(["function", "time_percentage"])
    )


    count_exprs = [pl.col(f"count_{func}").alias(func) for func in valid_functions]

    # Create the Count Long dataframe
    df_count_long = (
        _df.select([pl.col("instance")] + count_exprs)
        .unpivot(
            index="instance", variable_name="function", value_name="call_count"
        )
        .select(["function", "call_count"])
    )
    # 1. Create expressions for the time percentages, properly aliased
    _time_pct_exprs = [
        (pl.col(f"time_{func}") / pl.col("time")).alias(f"time_percentage_{func}")
        for func in valid_functions
    ]

    # 2. Create expressions for the call counts, properly aliased
    _count_exprs = [
        pl.col(f"count_{func}").alias(f"call_count_{func}")
        for func in valid_functions
    ]

    # 3. Build the final wide table in one pass
    _df.select(
        [pl.col("instance"), pl.col("time")] + _time_pct_exprs + _count_exprs
    )
    return df_count_long, df_time_long


@app.cell(hide_code=True)
def _(df_count_long, df_time_long):
    # --- TIME PERCENTAGE CHART ---
    base_time = alt.Chart(df_time_long)

    time_bars = base_time.mark_bar().encode(
        x=alt.X(
            "time_percentage:Q",
            bin=alt.Bin(maxbins=20),
            title="",
            axis=alt.Axis(format="%"),
        ),
        y=alt.Y("count():Q", title="Number of Instances"),
        color=alt.Color("function:N", legend=None),
    )

    time_mean_line = base_time.mark_rule(
        color="red", strokeDash=[5, 5], strokeWidth=2
    ).encode(x=alt.X("mean(time_percentage):Q"))

    time_hist = (
        alt.layer(time_bars, time_mean_line)
        .properties(width=250, height=200)
        .facet(column=alt.Column("function:N", title="Function"))
    )

    # --- CALL COUNT CHART ---
    base_count = alt.Chart(df_count_long)

    # 1. The Histogram
    count_bars = base_count.mark_bar().encode(
        x=alt.X("call_count:Q", bin=alt.Bin(maxbins=20), title=""),
        y=alt.Y("count():Q", title="Number of Instances"),
        color=alt.Color("function:N", legend=None),
    )

    # 2. The Average Line
    count_mean_line = base_count.mark_rule(
        color="red", strokeDash=[5, 5], strokeWidth=2
    ).encode(x=alt.X("mean(call_count):Q"))

    # 3. Layer and Facet
    count_hist = (
        alt.layer(count_bars, count_mean_line)
        .properties(width=250, height=250)
        .facet(column=alt.Column("function:N", title="Function"))
        .resolve_scale(x="independent")
    )

    final_chart = alt.vconcat(time_hist, count_hist).resolve_scale(
        color="independent"
    )

    final_chart
    return


@app.cell
def _():
    return


@app.function
def compare_instance(df: pl.DataFrame, inst: str, src1: str, src2: str):
    # Filter and convert to dictionary
    s1 = df.filter(
        (pl.col("instance") == inst) & (pl.col("source") == src1)
    ).to_dicts()
    s2 = df.filter(
        (pl.col("instance") == inst) & (pl.col("source") == src2)
    ).to_dicts()

    if not s1 or not s2:
        return []

    s1, s2 = s1[0], s2[0]
    return [
        {"stat": col, src1: s1.get(col), src2: s2.get(col)}
        for col in df.columns
        if s1.get(col) or s2.get(col)
    ]


@app.function
def get_solved_with_time_factor(
    df: pl.DataFrame, source1: str, source2: str, factor: int
) -> pl.DataFrame:
    """
    Returns instances solved by both source1 and source2,
    where source1 was at least 'factor' times faster than source2.
    """

    # 1. Get solved instances for source1
    s1_solved = df.filter(
        (pl.col("source") == source1)
        & (pl.col("lb") == pl.col("ub"))
        & (pl.col("lb").is_not_null())
    ).select(
        [
            pl.col("instance"),
            pl.col("time").alias("time_s1"),
            pl.col("ub").alias("value_s1"),
        ]
    )

    # 2. Get solved instances for source2
    s2_solved = df.filter(
        (pl.col("source") == source2)
        & (pl.col("lb") == pl.col("ub"))
        & (pl.col("lb").is_not_null())
    ).select(
        [
            pl.col("instance"),
            pl.col("time").alias("time_s2"),
            pl.col("lb").alias("lb_s2"),
            pl.col("ub").alias("ub_s2"),
        ]
    )

    # 3. Join and filter for speedup factor
    return (
        s1_solved.join(s2_solved, on="instance", how="inner")
        .filter(pl.col("time_s2") >= (pl.col("time_s1") * factor))
        .select(["instance", "time_s1", "time_s2", "value_s1"])
    ).rename(
        {
            "time_s1": f"{source1} time",
            "time_s2": f"{source2} time",
            "value_s1": "colors",
        }
    )


@app.function
def get_exclusive_solved(
    df: pl.DataFrame, source1: str, source2: str
) -> pl.DataFrame:
    """
    Returns instances solved by source1 but not by source2.
    """
    # 1. Get instances solved by source1
    s1_solved = df.filter(
        (pl.col("source") == source1) & (pl.col("lb") == pl.col("ub"))
    ).select(
        [
            pl.col("instance"),
            pl.col("time").alias("time_s1"),
            pl.col("ub").alias(
                "value_s1"
            ),  # Since lb == ub, ub is the final value
        ]
    )

    # 2. Get all instances for source2
    s2_all = df.filter(pl.col("source") == source2).select(
        [
            pl.col("instance"),
            pl.col("lb").alias("lb_s2"),
            pl.col("ub").alias("ub_s2"),
        ]
    )

    # 3. Join and filter for instances where source2 did NOT solve it
    return (
        s1_solved.join(s2_all, on="instance", how="left")
        .filter(
            (pl.col("lb_s2") != pl.col("ub_s2")) | pl.col("lb_s2").is_null()
        )
        .select(["instance", "time_s1", "value_s1", "lb_s2", "ub_s2"])
    ).rename(
        {
            "time_s1": f"{source1} time",
            "value_s1": "colors",
            "lb_s2": f"{source2} LB",
            "ub_s2": f"{source2} UB",
        }
    )


@app.function
def alt_cumulative_relative_histogram(df: pl.DataFrame, base: str):
    """
    For each instance, compute the ratio of each source to the base source.
    ratio = time / time(base)
    Then plot the cumulative histogram of the ratios.
    """
    # 1. Compute the ratio using a window function
    # 2. Filter the result
    df_transformed = df.with_columns(
        ratio=pl.col("time")
        / pl.col("time")
        .filter(pl.col("source") == base)
        .first()
        .over("instance")
    ).filter(pl.col("ratio") <= 1000)

    # Note: Altair works seamlessly with Polars DataFrames
    return (
        altair_accu(df_transformed, x_axis="ratio", max_x=1000)
        .interactive()
        .properties(
            title=f"Cumulative Relative Histogram of Time (base = {base})"
        )
    )


@app.function
def get_stats(df: pl.DataFrame) -> dict:
    """
    For each source, count how many instances were solved (lb == ub)
    Also sum the time it took to solve those instances.
    """

    stats_df = df.group_by("source").agg(
        solved=pl.col("lb").eq(pl.col("ub")).sum(),
        total_time=pl.col("time").filter(pl.col("lb") == pl.col("ub")).sum(),
        geo_ave_time=(
            pl.col("time")
            .filter(pl.col("lb") == pl.col("ub"))
            .log()
            .mean()
            .exp()
        ),
    )
    _stats = stats_df.to_dict(as_series=False)

    sorted_indices = sorted(
        range(len(_stats["source"])),
        key=lambda i: _stats["solved"][i],
        reverse=True,
    )

    lines = []
    for i in sorted_indices:
        l = []
        for k in _stats:
            if k == "source":
                continue
            l.append(mo.stat(value=_stats[k][i], label=k))

        lines.append(
            mo.hstack(
                [
                    mo.md(f"**{_stats['source'][i]}**"),
                    mo.hstack(l, justify="center", gap="2rem"),
                ],
                align="center",
                justify="center",
            )
        )

    return mo.vstack(lines)


@app.function
def altair_accu(
    df: pl.DataFrame, x_axis: str = "time", max_x: float = 3600.0
) -> alt.Chart:
    # 1. Determine exactly which columns are needed for the visualization
    required_cols = {"instance", "source", x_axis}

    # Logic for tooltips matches your original logic
    tooltip_cols = ["instance:N", f"{x_axis}:Q", "source:N"]
    if x_axis == "gap":
        tooltip_cols += ["lb:Q", "ub:Q"]
        required_cols.update(["lb", "ub"])
    elif x_axis != "time":
        tooltip_cols += ["time:Q"]
        required_cols.add("time")

    # 2. Select only used columns and compute cumulative count
    # This removes the other 40+ unused columns from the payload
    cumulative_data = (
        df.select(list(required_cols))
        .sort(x_axis, nulls_last=True)
        .with_columns(cumulative=pl.int_range(1, pl.len() + 1).over("source"))
    )

    # === Define shared instance selection ===
    highlight = alt.selection_point(
        fields=["instance"], nearest=True, empty="all"
    )

    # === Scale logic ===
    # Use fill_null/clip to handle log scale issues if necessary
    min_val = cumulative_data[x_axis].min()
    if min_val is None or min_val <= 0.0:
        min_val = 0.001

    # === Base encoding ===
    base = alt.Chart(cumulative_data).encode(
        x=alt.X(
            f"{x_axis}:Q",
            scale=alt.Scale(type="log", domain=[min_val, max_x]),
            title=x_axis.capitalize(),
        ),
        y=alt.Y(
            "cumulative:Q",
            title="Cumulative Count",
        ),
        color="source:N",
        tooltip=tooltip_cols,
    )

    # === Line chart ===
    lines = base.mark_line(
        interpolate="step-after"
    )  # Step-after is standard for cumulative plots

    # === Points with highlighting ===
    points = (
        base.mark_point(size=50)
        .encode(
            opacity=alt.condition(highlight, alt.value(1.0), alt.value(0.2))
        )
        .add_params(highlight)
    )

    # === Compose and return chart ===
    return (
        (lines + points).properties(width=800, height=450)
        # .configure_axis(grid=True, gridOpacity=0.7)
    )


@app.function
def compute_ratio(df: pl.DataFrame, col: str = "time") -> pl.DataFrame:
    """
    For each instance, compute the ratio of each source to the best source.
    ratio = time / min(time)
    """
    return df.with_columns(
        ratio=pl.col(col) / pl.col(col).min().over("instance")
    ).with_columns(
        pl.col("ratio")
        .replace({float("inf"): None, float("-inf"): None})
        .fill_nan(None)
    )


@app.function(hide_code=True)
def get_csvs(root: Path | str = ".") -> list[Path]:
    """
    Get the list of all CSVs in the given root directory but not subdirectories.
    """
    root = Path(root).expanduser()
    if not root.is_dir():
        raise ValueError(f"{root} is not a directory")
    return list(root.glob("*.csv"))


@app.function(hide_code=True)
def compute_gap(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute the relative gap between 'lb' and 'ub' columns in the DataFrame.
    gap = (|ub| - |lb|) / |ub|
    Note that if ub = lb = 0, then the gap is defined to be zero. If ub = 0 and lb != 0, the gap is defined to be infinity.
    """
    required_cols = {"lb", "ub"}
    missing_cols = required_cols.difference(df.columns)
    if missing_cols:
        missing = ", ".join(sorted(missing_cols))
        raise ValueError(f"compute_gap requires columns: {missing}")

    ub_abs = pl.col("ub").abs()
    lb_abs = pl.col("lb").abs()
    return df.with_columns(
        pl.when(pl.col("ub").is_null() | pl.col("lb").is_null())
        .then(None)
        .when((pl.col("ub") == 0) & (pl.col("lb") == 0))
        .then(0.0)
        .when(pl.col("ub") == 0)
        .then(float("inf"))
        .otherwise((ub_abs - lb_abs) / ub_abs)
        .alias("gap")
    )


@app.function(hide_code=True)
def get_df(file: Path | str) -> pl.DataFrame:
    """
    Get the DataFrame from the given CSV file and add the column "source"
    """
    file = Path(file).expanduser()
    df = pl.read_csv(file)
    df = compute_gap(df)
    df = df.with_columns(pl.lit(file.stem).alias("source"))
    return df


@app.function(hide_code=True)
def concat_dfs(
    dfs: list[pl.DataFrame], instance_filter: pl.DataFrame | None = None
) -> pl.DataFrame:
    """
    Concatenate the DataFrames from the given list of CSV files, and filter to the common "instance" to all "source"
    """
    if not dfs:
        return pl.DataFrame()

    common_instances = set(dfs[0]["instance"].to_list())
    for d in dfs[1:]:
        common_instances.intersection_update(d["instance"].to_list())

    if instance_filter is not None:
        common_instances = common_instances.intersection(set(instance_filter))

    filtered_dfs = [
        d.filter(pl.col("instance").is_in(common_instances)) for d in dfs
    ]
    return pl.concat(filtered_dfs, how="diagonal_relaxed").with_columns(
        pl.col(pl.Float64, pl.Float32).round(6)
    )


@app.cell
def _():
    _metacsv = get_df(PROJECT_ROOT / "inst/metadata.csv")
    _primal = get_df(PROJECT_ROOT / "logs/primal_f3e4eb0_results.csv")
    _held = get_df(PROJECT_ROOT / "logs/held.csv")
    _ord = get_df(PROJECT_ROOT / "logs/ordering.csv")


    _metacsv = _metacsv.drop(
        [c for c in ["source", "gap"] if c in _metacsv.columns]
    ).rename({"lb": "known_lb", "ub": "known_ub"})


    _metacsv = _metacsv.with_columns(
        pl.when(
            (pl.col("known_lb") == pl.col("known_ub"))
            & pl.col("known_lb").is_not_null()
        )
        .then(pl.col("known_lb"))
        .otherwise(None)
        .alias("chi")
    )

    _metacsv = _metacsv.join(
        _ord.select(
            pl.col("instance").alias("instance"),
            pl.col("root_lb").ceil().alias("ord_root"),
            pl.col("root_time_seconds").alias("ord_root_time"),
            (pl.col("gap") == 0).alias("ord_solved"),
        ),
        on="instance",
        how="left",
    )

    _metacsv = _metacsv.join(
        _primal.select(
            pl.col("instance").alias("instance"),
            pl.col("ub").alias("root_ub"),
            pl.col("first_clique").alias("first_clique"),
            pl.col("time_clique").alias("time_clique"),
            (pl.col("gap") == 0).alias("held_solved"),
        ),
        on="instance",
        how="left",
    )
    meta = _metacsv
    return (meta,)


@app.cell
def _():
    _some_matilda_inst = (
        Path(PROJECT_ROOT / "inst/some-matilda")
        .expanduser()
        .read_text()
        .splitlines()
    )

    filters = {
        "source": {
            "DIMACS": pl.col("instance").str.starts_with("g").not_(),
            "MATILDA": pl.col("instance").str.starts_with("g"),
            "SOME-MATILDA": pl.col("instance").is_in(_some_matilda_inst),
        },
        "chi_f vs chi": {
            "chi_f == chi": pl.col("chi_f") == pl.col("chi"),
            "chi_f < chi": pl.col("chi_f") < pl.col("chi"),
        },
        "chi vs root_ub": {
            "chi == root_ub": pl.col("chi") == pl.col("root_ub"),
            "chi < root_ub": pl.col("chi") < pl.col("root_ub"),
        },
        "chi_f vs ord_root": {
            "chi_f == ord_root": pl.col("chi_f") == pl.col("ord_root"),
            "chi_f < ord_root": pl.col("chi_f") < pl.col("ord_root"),
            "chi_f > ord_root": pl.col("chi_f") > pl.col("ord_root"),
        },
        "ord_root vs first_clique": {
            "ord_root > first_clique and faster": (
                (pl.col("ord_root") > pl.col("first_clique"))
                & (pl.col("ord_root_time") < pl.col("time_clique"))
            ),
            "ord_root > first_clique, but slower": (
                (pl.col("ord_root") > pl.col("first_clique"))
                & (pl.col("ord_root_time") > pl.col("time_clique"))
            ),
            "ord_root < first_clique and faster": (
                (pl.col("ord_root") < pl.col("first_clique"))
                & (pl.col("ord_root_time") < pl.col("time_clique"))
            ),
            "ord_root < first_clique, but slower": (
                (pl.col("ord_root") < pl.col("first_clique"))
                & (pl.col("ord_root_time") > pl.col("time_clique"))
            ),
        },
        "who solves it?": {
            "solved by held": pl.col("held_solved") == True,
            "solved by ord": pl.col("ord_solved") == True,
        },
    }
    return (filters,)


@app.cell
def _(filters, meta):
    switches = {
        group_name: {
            name: mo.ui.switch(
                # Filter 'meta' using the expression, select 'instance', and count unique values
                label=f"{meta.filter(expr).select(pl.col('instance')).n_unique()}"
            )
            for name, expr in group_exprs.items()
        }
        for group_name, group_exprs in filters.items()
    }
    return (switches,)


@app.cell
def _(switches):
    switch_dict = mo.ui.dictionary(
        {
            group_name: mo.ui.dictionary(group_switches, label=" ")
            for group_name, group_switches in switches.items()
        }
    )
    return (switch_dict,)


@app.function(hide_code=True)
def apply_marimo_expressions(df, expr_filters, switches):
    and_conditions = []

    for group_name, group_exprs in expr_filters.items():
        # Get the active expressions for this group
        active_exprs = [
            expr
            for name, expr in group_exprs.items()
            if switches[group_name][name].value
        ]

        if not active_exprs:
            continue

        # Combine active expressions in this group with OR (|)
        combined_or_expr = reduce(operator.or_, active_exprs)
        and_conditions.append(combined_or_expr)

    # If there are active groups, combine them all with AND (&) and filter once
    if and_conditions:
        final_expr = reduce(operator.and_, and_conditions)
        return df.filter(final_expr)

    return df


@app.cell
def _(filters, meta, switch_dict):
    # get the list of unique instances in the filtered meta
    filter_instances = (
        apply_marimo_expressions(meta, filters, switch_dict)["instance"]
        .unique()
        .to_list()
    )
    return (filter_instances,)


if __name__ == "__main__":
    app.run()

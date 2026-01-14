#!/Users/ieremies/.local/bin/uvx marimo edit

import marimo

__generated_with = "0.19.2"
app = marimo.App(width="columns")


@app.cell(column=0)
def _():
    from pathlib import Path
    from typing import List

    import altair as alt
    import marimo as mo
    import numpy as np
    import polars as pl

    PROJECT_ROOT = Path("/Users/ieremies/proj/color")
    return List, PROJECT_ROOT, Path, alt, mo, np, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Ideias:
    - usar o `mo.stat` para visualizar métricas principais de desempenho.
    - filtrar o dataframe para apenas as colunas que são importantes. O altair coloca todo o dataframe no payload, o que pode ser um problema se houver muitos dados.
    - usar o `mo.watch.directory` para atualizar a lista de arquivos.
    - colocar este notebook para ser servido na `opt3`: coloca numa porta alta e faz `ssh -L 9000:127.0.0.1:8080 ieremies@opt3`.
    - Eu posso gastar muito tempo no `clique` sem "precisar". Por exemplo, `DSJC1000.9` gasta 800s para achar um clique de 60 sendo que o menor grau é 870.
    - Porque as instâncias `mug` precisam do LS para resolver?
    - quem está gerando os UB?

    ## Memória

    As seguintes instâncias aprensemtaram "problemas de memória". As duas primeiras utilizaram memória de mais, o resto foi morto pelo limite de nós do _branch-and-reduce_. Em especial, `flat_300_28_0` chegou a utilizar 13Gb mesmo sendo morto aos 436s.

    ```
    DSJC1000.9
    flat300_28_0
    ---
    2-Insertions_5
    3-Insertions_4
    4-Insertions_4
    DSJC250.1
    DSJC500.1
    DSJC500.1
    DSJC500.5
    ash331GPIA
    le450_15b
    le450_15c
    le450_15d
    le450_25c
    le450_25d
    le450_5b
    le450_5c
    le450_5d
    queen12_12
    queen13_13
    queen14_14
    queen15_15
    queen16_16
    wap06a
    ```
    """)
    return


@app.cell(hide_code=True)
def _(List, Path):
    def get_csvs(root: Path | str = ".") -> List[Path]:
        """
        Get the list of all CSVs in the given root directory and its subdirectories.
        """
        root = Path(root)
        root.expanduser()
        return list(root.glob("*.csv"))
    return (get_csvs,)


@app.cell(hide_code=True)
def _(pl):
    def compute_gap(df: pl.DataFrame) -> pl.DataFrame:
        """
        Compute the relative gap between 'lb' and 'ub' columns in the DataFrame.
        gap = (|ub| - |lb|) / |ub|
        Note that if ub = lb = 0, then the gap is defined to be zero. If ub = 0 and lb != 0, the gap is defined to be infinity.
        """
        return df.with_columns(
            pl.when((pl.col("ub") == 0) & (pl.col("lb") == 0))
            .then(0.0)
            .otherwise(
                (pl.col("ub").abs() - pl.col("lb").abs()) / pl.col("ub").abs()
            )
            .alias("gap")
        )
    return (compute_gap,)


@app.cell(hide_code=True)
def _(Path, compute_gap, pl):
    def get_df(file: Path) -> pl.DataFrame:
        """
        Get the DataFrame from the given CSV file and add the column "source"
        """
        df = pl.read_csv(file)
        df = compute_gap(df)
        df = df.with_columns(pl.lit(file.stem).alias("source"))
        return df
        ...
    return (get_df,)


@app.cell(hide_code=True)
def _(List, pl):
    def concat_dfs(dfs: List[pl.DataFrame], filter=None) -> pl.DataFrame:
        """
        Concatenate the DataFrames from the given list of CSV files, and filter to the common "instance" to all "source"
        """
        common_instances = set.intersection(
            *[set(d["instance"].to_list()) for d in dfs]
        )

        if filter is not None:
            common_instances = common_instances.intersection(
                set(filter["instance"].to_list())
            )

        filtered_dfs = [
            d.filter(pl.col("instance").is_in(common_instances)) for d in dfs
        ]
        return pl.concat(filtered_dfs, how="diagonal_relaxed").with_columns(
            pl.col(pl.Float64, pl.Float32).round(6)
        )
        ...
    return (concat_dfs,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Sources
    """)
    return


@app.cell(hide_code=True)
def _(PROJECT_ROOT, get_csvs, mo):
    # TODO add mo.watch.directory
    csvs_files = {
        str(n).replace(str(PROJECT_ROOT), ""): n
        for n in get_csvs(PROJECT_ROOT / "logs")
        + get_csvs(PROJECT_ROOT / "inst/lit")
    }
    files = mo.ui.multiselect(
        csvs_files,
        label="Select CSV files to plot",
        value=["/logs/held.csv", "/logs/ordering.csv"],
    )
    files
    return csvs_files, files


@app.cell(hide_code=True)
def _(concat_dfs, csvs_files, files, get_df, instance_filter, mo):
    mo.stop(csvs_files is None or len(files.value) == 0, "No files selected.")

    df = concat_dfs([get_df(f) for f in files.value], instance_filter.value)

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
def _(mo):
    mo.md(r"""
    # Graphs

    Overview of the results.

    - Total time: must be the total time of instances that are solved by all
    - Same for the average time.
    - Count the number of roots solved.
    - Count the number of root_ub == opt
    """)
    return


@app.cell(hide_code=True)
def _(pl):
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
        return stats_df.to_dict(as_series=False)
    return (get_stats,)


@app.cell(hide_code=True)
def _(df, get_stats, mo):
    _stats = get_stats(df)

    lines = []
    for i, _ in enumerate(_stats["source"]):
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

    mo.vstack(lines)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Accumulated Distribution

    The idea is:
    > By the minute X, how many of the instances were solved?
    """)
    return


@app.cell(hide_code=True)
def _(alt, pl):
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
            (lines + points)
            .properties(width=800, height=450)
            .configure_axis(grid=True, gridOpacity=0.7)
        )
    return (altair_accu,)


@app.cell
def _(altair_accu, df):
    altair_accu(df, x_axis="time", max_x=3600.0).interactive().properties(
        title="Cumulative Time to Solve"
    )
    return


@app.cell
def _(altair_accu, df):
    altair_accu(df, x_axis="gap", max_x=3600.0).interactive().properties(
        title="Cumulative Gap"
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Performance Profile
    """)
    return


@app.cell
def _(pl):
    def compute_ratio(df: pl.DataFrame, col: str = "time") -> pl.DataFrame:
        """
        For each instance, compute the ratio of each source to the best source.
        ratio = time / min(time)
        """
        return df.with_columns(
            ratio=pl.col(col) / pl.col(col).min().over("instance")
        )
    return (compute_ratio,)


@app.cell
def _(altair_accu, compute_ratio, df):
    altair_accu(
        compute_ratio(df), x_axis="ratio", max_x=1000
    ).interactive().properties(title="Performance Profile of the Time to Solve")
    return


@app.cell
def _(altair_accu, compute_ratio, df):
    altair_accu(
        compute_ratio(df, "gap"), x_axis="ratio", max_x=10
    ).interactive().properties(title="Performance Profile of the Gap")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Histogram
    """)
    return


@app.cell(hide_code=True)
def _(df, mo):
    _sources = df["source"].unique().to_list()
    _default = "held" if "held" in _sources else _sources[0]
    base_histogram = mo.ui.radio(
        _sources, label="Which source to use as baseline?", value=_default
    )
    base_histogram
    return (base_histogram,)


@app.cell(hide_code=True)
def _(altair_accu, pl):
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
    return (alt_cumulative_relative_histogram,)


@app.cell(hide_code=True)
def _(alt_cumulative_relative_histogram, base_histogram, df, mo):
    chart_histo = mo.ui.altair_chart(
        alt_cumulative_relative_histogram(df, base_histogram.value)
    )
    return (chart_histo,)


@app.cell
def _(chart_histo, mo):
    mo.vstack([chart_histo, chart_histo.value])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Outliers
    """)
    return


@app.cell(hide_code=True)
def _(df, mo, np):
    _sources = df["source"].unique().to_list()
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
def _(pl):
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
    return (get_exclusive_solved,)


@app.cell(hide_code=True)
def _(pl):
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
    return (get_solved_with_time_factor,)


@app.cell(hide_code=True)
def _(
    df,
    get_exclusive_solved,
    get_solved_with_time_factor,
    mo,
    out_source1,
    out_source2,
    time_cutoff,
):
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
def _(pl):
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
    return (compare_instance,)


@app.cell(hide_code=True)
def _(df, mo):
    _instances = sorted(df["instance"].unique().to_list())

    instance_detail = mo.ui.dropdown(
        options=_instances,
        value=_instances[0],
        label="Select an instance to look closer: ",
    )
    instance_detail
    return (instance_detail,)


@app.cell(hide_code=True)
def _(compare_instance, df, instance_detail, mo, out_source1, out_source2):
    _aux = compare_instance(
        df, instance_detail.value, out_source1.value, out_source2.value
    )

    mo.vstack([mo.hstack(l, align="start") for l in _aux])
    mo.ui.table(_aux, pagination=False, selection=None)
    return


@app.cell
def _():
    return


@app.cell
def _(mo):
    mo.md(r"""
 
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### `flat_300_28_0`

    ```
    2839   │ ( 184.446s)       utils.hpp:51    INFO| .   .   .   0.457846 s: MWISheuristic
    2840   │ ( 292.408s)       utils.hpp:49    INFO| .   .   .   107.960207 s: branch_reduce -> 292 sets, alpha = 1.19751
    2841   │ ( 292.836s)      solver.cpp:212   INFO| .   .   .   0.419250 s: GRB_optimize  -> 27.978095 | 6061 sets
    2842   │ ( 292.843s)       utils.hpp:49    INFO| .   .   .   0.001075 s: MWISheuristic -> weighted = 1.02984
    2843   │ ( 292.898s)      solver.cpp:212   INFO| .   .   .   0.053140 s: GRB_optimize  -> 27.977151 | 6062 sets
    2844   │ ( 292.905s)       utils.hpp:49    INFO| .   .   .   0.001414 s: MWISheuristic -> weighted = 1.0089
    2845   │ ( 292.927s)      solver.cpp:212   INFO| .   .   .   0.020694 s: GRB_optimize  -> 27.977101 | 6063 sets
    2846   │ ( 292.934s)       utils.hpp:49    INFO| .   .   .   0.001060 s: MWISheuristic -> weighted = 1.06442
    2847   │ ( 293.010s)      solver.cpp:212   INFO| .   .   .   0.074974 s: GRB_optimize  -> 27.972247 | 6064 sets
    2848   │ ( 293.017s)       utils.hpp:49    INFO| .   .   .   0.001105 s: MWISheuristic -> weighted = 1.03891
    2849   │ ( 293.072s)      solver.cpp:212   INFO| .   .   .   0.054600 s: GRB_optimize  -> 27.970979 | 6065 sets
    2850   │ ( 293.080s)       utils.hpp:49    INFO| .   .   .   0.001682 s: MWISheuristic -> weighted = 1.07412
    2851   │ ( 293.171s)      solver.cpp:212   INFO| .   .   .   0.090111 s: GRB_optimize  -> 27.965069 | 6066 sets
    2852   │ ( 293.178s)       utils.hpp:49    INFO| .   .   .   0.001054 s: MWISheuristic -> weighted = 1.00662
    2853   │ ( 293.215s)      solver.cpp:212   INFO| .   .   .   0.036109 s: GRB_optimize  -> 27.964815 | 6067 sets
    2854   │ ( 293.677s)       utils.hpp:51    INFO| .   .   .   0.454566 s: MWISheuristic
    2855   │ ( 436.290s)       utils.hpp:49    INFO| .   .   .   142.612261 s: branch_reduce -> 271 sets, alpha = 1.11356
    2856   │ ( 436.296s)      solver.cpp:297   WARN| .   .   .   New (safe) LB[ 26 , 40 ]
    2857   │ ( 436.780s)      solver.cpp:212   INFO| .   .   .   0.477580 s: GRB_optimize  -> 27.791079 | 6338 sets
    2858   │ ( 436.790s)       utils.hpp:51    INFO| .   .   .   0.003106 s: MWISheuristic
    2859   │ ( 485.714s)      solver.cpp:47     ERR| .   .   .   Stopping to preserve memory
    2860   │ ( 485.714s)      loguru.cpp:637   INFO| .   .   .   atexit
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Separação

    Aqui estão as instâncias que utilizaram a separação de alguma forma:
    """)
    return


@app.cell
def _(df, pl):
    # instances where "count_Expand" > 1
    separation_instances = df.filter(pl.col("count_Expand") > 1)[
        "instance"
    ].unique()
    df.filter(pl.col("count_Expand") > 1)
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell(column=1)
def _():
    return


@app.cell(hide_code=True)
def _(pl):
    meta = (
        pl.read_csv("~/proj/color/inst/metadata.csv")
        .drop(
            [
                "algebraic_connectivity",
                "energy",
                "matilda_maxis",
                "matilda_dsatur",
                "source",
                "components",
                "components_complement",
            ]
        )
        # drop lines where the "instance" starts with "g"
        .filter(~pl.col("instance").str.starts_with("g"))
        # add a column "solved" = pl.col("lb") == pl.col("ub")
        .with_columns((pl.col("lb") == pl.col("ub")).alias("solved"))
    )
    meta
    return (meta,)


@app.cell(hide_code=True)
def _(Path, concat_dfs, get_df, get_exclusive_solved, meta, pl):
    _df = concat_dfs(
        [
            get_df(f)
            for f in [
                Path("/Users/ieremies/proj/color/logs/held.csv"),
                Path("/Users/ieremies/proj/color/logs/ordering.csv"),
            ]
        ]
    )
    all = _df.select(pl.col("instance")).unique()

    frac_is_opt = (
        _df.select(pl.col("instance"), pl.col("root_lb"), pl.col("source"))
        .filter(pl.col("source") == "held")
        .join(
            meta.select(pl.col("instance"), pl.col("lb"), pl.col("ub")),
            on="instance",
            how="inner",
        )
        .with_columns(pl.col("root_lb").cast(pl.Float64).alias("_root_lb_numeric"))
        .filter(
            (pl.col("lb") == pl.col("ub"))
            & (pl.col("ub").cast(pl.Float64) == pl.col("_root_lb_numeric"))
        )
        .select("instance")
        .unique()
    )

    root_ub_is_opt = (
        _df.select(pl.col("instance"), pl.col("root_ub"), pl.col("source"))
        .filter(pl.col("source") == "held")
        .join(
            meta.select(pl.col("instance"), pl.col("lb"), pl.col("ub")),
            on="instance",
            how="inner",
        )
        .with_columns(pl.col("root_ub").cast(pl.Float64).alias("_root_ub_numeric"))
        .filter(
            (pl.col("lb") == pl.col("ub"))
            & (pl.col("ub").cast(pl.Float64) == pl.col("_root_ub_numeric"))
        )
        .select("instance")
        .unique()
    )

    # Instances that should be solved on the root node.
    instance_set_root = frac_is_opt.join(
        root_ub_is_opt, on="instance", how="inner"
    )

    # Instances that have fractional chromatic number equal to chromatic number, but are missing an UB
    instance_set_missing_ub = frac_is_opt.filter(
        ~pl.col("instance").is_in(instance_set_root["instance"].to_list())
    )

    # Instances that have root_ub equal to chromatic number, but are missing a fractional lower bound
    instance_set_missing_frac_lb = root_ub_is_opt.filter(
        ~pl.col("instance").is_in(instance_set_root["instance"].to_list())
    )

    # Instances that are missing both lower and upper bounds after solving the root
    instance_set_missing_both = all.filter(
        ~pl.col("instance").is_in(
            instance_set_root["instance"].to_list()
            + instance_set_missing_ub["instance"].to_list()
            + instance_set_missing_frac_lb["instance"].to_list()
        )
    )

    # Instances solved by one but not the other
    solved_by_held = get_exclusive_solved(_df, "held", "ordering")
    solved_by_ordering = get_exclusive_solved(_df, "ordering", "held")

    # Instance where "ordering" "root_obj" is greater than "held" "root_lb"
    ordering_better_root_lb = (
        _df.filter(pl.col("source") == "ordering")
        .join(
            _df.filter(pl.col("source") == "held"),
            on="instance",
            how="inner",
            suffix="_held",
        )
        .filter(pl.col("root_obj").ceil() > pl.col("root_lb_held"))
        .select(["instance"])
        .unique()
    )

    # Instance where "ordering" "root_obj" is equal to "held" "root_lb"
    # and "ordering" "root_time_seconds" is less than "held" "root_time"
    ordering_faster_root_lb = (
        _df.filter(pl.col("source") == "ordering")
        .join(
            _df.filter(pl.col("source") == "held"),
            on="instance",
            how="inner",
            suffix="_held",
        )
        .filter(
            (pl.col("root_obj").ceil() == pl.col("root_lb_held"))
            & (pl.col("time") < pl.col("time_held"))
        )
        .select(["instance"])
        .unique()
    )

    _my_df = get_df(Path("/Users/ieremies/proj/color/logs/escovacao_base.csv"))

    # Instances where "ordering" "root_obj" is greater than to _my_df "first_clique"
    ordering_better_than_my_clique = (
        _df.filter(pl.col("source") == "ordering")
        .join(
            _my_df.select(
                [
                    pl.col("instance"),
                    pl.col("first_clique").alias("my_first_clique"),
                    pl.col("time_clique"),
                ]
            ),
            on="instance",
            how="inner",
        )
        .filter(pl.col("root_obj").ceil() > pl.col("my_first_clique"))
        .select(
            [
                "instance",
                "root_obj",
                "root_time_seconds",
                "my_first_clique",
                "time_clique",
            ]
        )
    )
    ordering_better_than_my_clique

    # Instances where "ordering" "root_obj" is faster than _my_df "first_clique"
    ordering_faster_than_my_clique = (
        _df.filter(pl.col("source") == "ordering")
        .join(
            _my_df.select(
                [
                    pl.col("instance"),
                    pl.col("first_clique").alias("my_first_clique"),
                    pl.col("time_clique"),
                ]
            ),
            on="instance",
            how="inner",
        )
        .filter(
            (pl.col("root_obj").ceil() == pl.col("my_first_clique"))
            & (pl.col("root_time_seconds") < pl.col("time_clique"))
        )
        .select(
            [
                "instance",
                "root_obj",
                "root_time_seconds",
                "my_first_clique",
                "time_clique",
            ]
        )
    )
    ordering_faster_than_my_clique
    return (
        all,
        instance_set_missing_both,
        instance_set_missing_frac_lb,
        instance_set_missing_ub,
        instance_set_root,
        ordering_better_root_lb,
        ordering_better_than_my_clique,
        ordering_faster_root_lb,
        ordering_faster_than_my_clique,
        solved_by_held,
        solved_by_ordering,
    )


@app.cell
def _(
    all,
    instance_set_missing_both,
    instance_set_missing_frac_lb,
    instance_set_missing_ub,
    instance_set_root,
    mo,
    ordering_better_root_lb,
    ordering_better_than_my_clique,
    ordering_faster_root_lb,
    ordering_faster_than_my_clique,
    solved_by_held,
    solved_by_ordering,
):
    instance_filter = mo.ui.radio(
        {
            f"{len(all):>3} | all": all,
            f"{len(instance_set_root):>3} | both root_ub and root_lb are optimal at root": instance_set_root,
            f"{len(instance_set_missing_ub):>3} | frac chrom is the optimal, but UB is missing": instance_set_missing_ub,
            f"{len(instance_set_missing_frac_lb):>3} | UB at root is the optimal, but LB is missing": instance_set_missing_frac_lb,
            f"{len(instance_set_missing_both):>3} | both LB and UB are missing at root": instance_set_missing_both,
            f"{len(solved_by_held):>3} | solved by held but not by ordering": solved_by_held,
            f"{len(solved_by_ordering):>3} | solved by ordering but not by held": solved_by_ordering,
            f"{len(ordering_better_root_lb):>3} | ordering has better root_lb than held": ordering_better_root_lb,
            f"{len(ordering_faster_root_lb):>3} | ordering matches held root_lb but is faster": ordering_faster_root_lb,
            f"{len(ordering_better_than_my_clique):>3} | ordering has better root_obj than my first_clique": ordering_better_than_my_clique,
            f"{len(ordering_faster_than_my_clique):>3} | ordering matches my first_clique but is faster": ordering_faster_than_my_clique,
        },
        label="Select which set of instances to view:",
        value=f"{len(all):>3} | all",
    )
    instance_filter
    return (instance_filter,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

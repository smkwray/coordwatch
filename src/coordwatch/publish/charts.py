
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def plot_alignment_timeline(weekly: pd.DataFrame, quarter: pd.DataFrame, out_path: Path) -> None:
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(pd.to_datetime(weekly["week"]), weekly["fed_pressure_dv01"], label="Fed pressure (weekly)")
    ax1.set_ylabel("Fed pressure proxy")
    ax1.set_xlabel("Week")
    ax1.axhline(0, linewidth=1)

    ax2 = ax1.twinx()
    ax2.step(pd.to_datetime(quarter["refunding_date"]), quarter["mix_shock_dv01"], where="post", label="Treasury mix shock (quarterly)")
    ax2.set_ylabel("Mix shock")

    lines, labels = [], []
    for ax in [ax1, ax2]:
        h, l = ax.get_legend_handles_labels()
        lines.extend(h)
        labels.extend(l)
    ax1.legend(lines, labels, loc="upper left")
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def plot_episode_quadrants(quarter: pd.DataFrame, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.scatter(quarter["expected_soma_redemptions_dv01"], quarter["mix_shock_dv01"])
    ax.axhline(0, linewidth=1)
    ax.axvline(0, linewidth=1)
    ax.set_xlabel("Expected SOMA redemptions / Fed pressure proxy")
    ax.set_ylabel("Treasury mix shock")
    for _, row in quarter.tail(12).iterrows():
        ax.annotate(str(row["quarter"]), (row["expected_soma_redemptions_dv01"], row["mix_shock_dv01"]), fontsize=7)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def plot_irf(lp_table: pd.DataFrame, term: str, title: str, out_path: Path) -> None:
    data = lp_table.loc[lp_table["term"] == term].copy().sort_values("horizon")
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(data["horizon"], data["coef"], marker="o")
    ax.fill_between(data["horizon"], data["ci_lower_95"], data["ci_upper_95"], alpha=0.2)
    ax.axhline(0, linewidth=1)
    ax.set_title(title)
    ax.set_xlabel("Horizon (weeks)")
    ax.set_ylabel("Response")
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160)
    plt.close(fig)

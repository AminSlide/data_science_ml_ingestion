"""Style commun des figures : palette validée pour les daltoniens, encre et grille discrètes."""

import matplotlib.pyplot as plt

from src.config import FIGURES

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
# Gris de mise en retrait : « rien », « autre », contexte.
GRAY = "#c3c2b7"
# Ordre fixe des couleurs de séries (vérifié avec un simulateur de daltonisme) : on prend les
# couleurs dans cet ordre, sans en générer d'autres. Les trois claires (vert d'eau, jaune, rose)
# contrastent peu avec le fond : chaque figure est accompagnée de son tableau.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
# Deux nuances d'un même bleu, pour comparer deux états d'une même chose (avant / après).
BLUE_LIGHT, BLUE_DARK = "#86b6ef", "#184f95"


def use_style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "font.family": "sans-serif",
            "font.sans-serif": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
            "font.size": 10,
            "text.color": INK,
            "axes.titlesize": 12,
            "axes.titleweight": "semibold",
            "axes.titlelocation": "left",
            "axes.titlepad": 12,
            "axes.labelcolor": INK_SECONDARY,
            "axes.edgecolor": BASELINE,
            "axes.linewidth": 0.8,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "grid.linestyle": "-",
            "xtick.color": BASELINE,
            "ytick.color": BASELINE,
            "xtick.labelcolor": INK_SECONDARY,
            "ytick.labelcolor": INK_SECONDARY,
            "legend.frameon": False,
            "axes.prop_cycle": plt.cycler(color=SERIES),
            "lines.linewidth": 2,
            "lines.solid_capstyle": "round",
            "lines.solid_joinstyle": "round",
        }
    )


def save(fig: plt.Figure, name: str) -> None:
    fig.savefig(FIGURES / f"{name}.png", dpi=200, bbox_inches="tight")

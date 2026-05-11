#!/usr/bin/env python3
"""Letterbox aerostream-architecture-dark.png into LinkedIn article image sizes."""
from pathlib import Path

try:
    from PIL import Image
except ImportError as e:
    raise SystemExit(
        "Pillow required: pip install pillow\n"
        "Or: pip install pillow --target docs/diagrams/.pillow_vendor"
    ) from e

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "aerostream-architecture-dark.png"
BG = (30, 30, 30)  # align with Mermaid dark theme canvas


def fit(src_path: Path, out_path: Path, li_w: int, li_h: int) -> None:
    img = Image.open(src_path).convert("RGBA")
    w, h = img.size
    scale = min(li_w / w, li_h / h)
    nw, nh = int(w * scale), int(h * scale)
    img_r = img.resize((nw, nh), Image.Resampling.LANCZOS)
    canvas = Image.new("RGBA", (li_w, li_h), BG + (255,))
    ox = (li_w - nw) // 2
    oy = (li_h - nh) // 2
    canvas.paste(img_r, (ox, oy), img_r)
    canvas.convert("RGB").save(out_path, "PNG", optimize=True)


def main() -> None:
    if not SRC.is_file():
        raise SystemExit(f"Missing source render: {SRC} (run mermaid-cli dark PNG first)")
    fit(SRC, ROOT / "aerostream-architecture-linkedin-1200x627-dark.png", 1200, 627)
    fit(SRC, ROOT / "aerostream-architecture-linkedin-2400x1254-dark@2x.png", 2400, 1254)


if __name__ == "__main__":
    main()

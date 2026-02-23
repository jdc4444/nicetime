#!/usr/bin/env python3
"""Build high-resolution globe textures from NOAA ETOPO1 NetCDF grid."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
from PIL import Image
from scipy.io import netcdf_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to ETOPO1 NetCDF grid (e.g. ETOPO1_Bed_g_gmt4.grd)",
    )
    parser.add_argument(
        "--height-out",
        required=True,
        type=Path,
        help="Output grayscale height map path (.png)",
    )
    parser.add_argument(
        "--normal-out",
        required=False,
        type=Path,
        help="Output tangent-space normal map path (.png). Omit to skip normal map generation.",
    )
    parser.add_argument("--width", type=int, default=8192, help="Output texture width")
    parser.add_argument("--height", type=int, default=4096, help="Output texture height")
    parser.add_argument(
        "--normal-strength",
        type=float,
        default=3.6,
        help="Scale factor for generated normal map strength",
    )
    return parser.parse_args()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def build_resampled_grid(src_grid, out_width: int, out_height: int) -> np.ndarray:
    src_height, src_width = src_grid.shape

    src_x = np.linspace(0.0, src_width - 1.0, out_width, dtype=np.float64)
    x0 = np.floor(src_x).astype(np.int32)
    x1 = np.minimum(x0 + 1, src_width - 1)
    wx = (src_x - x0).astype(np.float32)

    out = np.empty((out_height, out_width), dtype=np.float32)

    for y in range(out_height):
        src_y = y * (src_height - 1) / max(1, out_height - 1)
        y0 = int(np.floor(src_y))
        y1 = min(y0 + 1, src_height - 1)
        wy = np.float32(src_y - y0)

        row0 = src_grid[y0].astype(np.float32, copy=False)
        row1 = src_grid[y1].astype(np.float32, copy=False)

        r0 = row0[x0] * (1.0 - wx) + row0[x1] * wx
        r1 = row1[x0] * (1.0 - wx) + row1[x1] * wx
        out[y] = r0 * (1.0 - wy) + r1 * wy

        if y % 256 == 0:
            print(f"Resampling row {y}/{out_height}")

    return out


def elevation_to_height_u8(elevation_m: np.ndarray) -> tuple[np.ndarray, float, float]:
    min_e = float(np.nanmin(elevation_m))
    max_e = float(np.nanmax(elevation_m))
    pos_scale = max(max_e, 1.0)
    neg_scale = max(abs(min_e), 1.0)

    normalized = np.empty_like(elevation_m, dtype=np.float32)
    non_negative = elevation_m >= 0
    normalized[non_negative] = 0.5 + (elevation_m[non_negative] / (2.0 * pos_scale))
    normalized[~non_negative] = 0.5 + (elevation_m[~non_negative] / (2.0 * neg_scale))
    normalized = np.clip(normalized, 0.0, 1.0)
    return np.round(normalized * 255.0).astype(np.uint8), min_e, max_e


def build_normal_rgb(height_u8: np.ndarray, strength: float) -> np.ndarray:
    h = height_u8.astype(np.float32) / 255.0

    # Horizontal wraps to avoid seams at +/-180 longitude.
    dx = np.roll(h, -1, axis=1) - np.roll(h, 1, axis=1)

    # Vertical clamps at the poles.
    dy = np.empty_like(h)
    dy[0] = h[1] - h[0]
    dy[-1] = h[-1] - h[-2]
    dy[1:-1] = h[2:] - h[:-2]

    nx = -dx * strength
    ny = -dy * strength
    nz = np.ones_like(h, dtype=np.float32)

    inv_len = 1.0 / np.maximum(np.sqrt(nx * nx + ny * ny + nz * nz), 1e-6)
    nx *= inv_len
    ny *= inv_len
    nz *= inv_len

    normal = np.empty((h.shape[0], h.shape[1], 3), dtype=np.uint8)
    normal[..., 0] = np.round((nx * 0.5 + 0.5) * 255.0).astype(np.uint8)
    normal[..., 1] = np.round((ny * 0.5 + 0.5) * 255.0).astype(np.uint8)
    normal[..., 2] = np.round((nz * 0.5 + 0.5) * 255.0).astype(np.uint8)
    return normal


def save_height(path: Path, height_u8: np.ndarray) -> None:
    ensure_parent(path)
    Image.fromarray(height_u8).save(path, optimize=True)


def save_normal(path: Path, normal_rgb: np.ndarray) -> None:
    ensure_parent(path)
    Image.fromarray(normal_rgb).save(path, optimize=True)


def main() -> None:
    args = parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Missing source file: {args.input}")

    print(f"Loading {args.input}")
    with netcdf_file(str(args.input), mode="r", mmap=False) as ds:
        if "z" not in ds.variables:
            raise KeyError("NetCDF variable 'z' not found")
        if "y" not in ds.variables:
            raise KeyError("NetCDF variable 'y' not found")

        z = ds.variables["z"].data
        y = ds.variables["y"].data
        print(f"Source grid: {z.shape} {z.dtype}")

        elevation = build_resampled_grid(z, args.width, args.height)

    if float(y[0]) < float(y[-1]):
        # ETOPO1 files can be stored south->north; image textures must be north-up.
        elevation = np.flipud(elevation)
        print("Applied vertical flip to enforce north-up texture orientation.")

    print("Converting elevation to grayscale height")
    height_u8, min_e, max_e = elevation_to_height_u8(elevation)
    print(f"Normalization anchors: min {min_e:.2f} m, max {max_e:.2f} m")

    print(f"Saving height map: {args.height_out}")
    save_height(args.height_out, height_u8)

    if args.normal_out:
        print("Generating normal map")
        normal_rgb = build_normal_rgb(height_u8, args.normal_strength)
        print(f"Saving normal map: {args.normal_out}")
        save_normal(args.normal_out, normal_rgb)
    else:
        print("Skipping normal map generation (no --normal-out provided).")

    print("Done")


if __name__ == "__main__":
    main()

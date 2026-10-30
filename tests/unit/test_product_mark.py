"""One theme-aware plane and arrow-layer mark for the console, sign-in and offline catalog."""

from importlib.resources import files
from xml.etree import ElementTree

import pytest

pytestmark = pytest.mark.unit


def luminance(value):
    channels = [int(value[n : n + 2], 16) / 255 for n in (1, 3, 5)]
    values = [v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4 for v in channels]
    return sum(v * w for v, w in zip(values, (0.2126, 0.7152, 0.0722), strict=True))


def test_product_mark_geometry_matches_favicon_and_contrasts_with_both_panels():
    root = files("etl_craft.core").joinpath("static")
    partial = ElementTree.fromstring(root.joinpath("mark.html").read_text())
    favicon = ElementTree.fromstring(root.joinpath("favicon.svg").read_text())
    ns = {"svg": "http://www.w3.org/2000/svg"}
    assert partial.attrib["viewBox"] == favicon.attrib["viewBox"] == "0 0 24 24"

    def shapes(svg):
        return [node.attrib["d"] for node in svg.findall(".//svg:path", ns)]

    assert shapes(partial) == shapes(favicon)
    assert len(shapes(partial)) == 4
    assert partial.attrib["stroke-linecap"] == partial.attrib["stroke-linejoin"] == "round"
    assert partial.attrib["aria-hidden"] == "true"
    assert "prefers-color-scheme: dark" in root.joinpath("favicon.svg").read_text()
    for tones, backgrounds in (
        (("#0969da", "#59636e"), ("#eef2f6", "#edf1f5")),
        (("#4493f8", "#9198a1"), ("#151b23", "#0d1117")),
    ):
        for tone in tones:
            for background in backgrounds:
                a, b = luminance(tone), luminance(background)
                assert (max(a, b) + 0.05) / (min(a, b) + 0.05) >= 3

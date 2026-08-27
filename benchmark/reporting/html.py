from __future__ import annotations

from pathlib import Path
import re

from .common import ReportGenerationError


HTML_CSS_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_benchmark_report.css"
HTML_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_benchmark_report.js"


def _report_asset_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ReportGenerationError(f"Unable to read report asset {path}: {exc}") from exc


def _html_esc(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _html_inline(value: str) -> str:
    placeholders: list[str] = []

    def _stash_code(match: re.Match[str]) -> str:
        placeholders.append(f'<code>{_html_esc(match.group(1))}</code>')
        return f'@@CODE{len(placeholders) - 1}@@'

    value = re.sub(r'`([^`]+)`', _stash_code, value)
    value = _html_esc(value)
    value = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', value)
    value = re.sub(r'(?<![A-Za-z0-9])_(?!\s)(.+?)(?<!\s)_(?![A-Za-z0-9])', r'<em>\1</em>', value)
    for idx, code_html in enumerate(placeholders):
        value = value.replace(f'@@CODE{idx}@@', code_html)
    return value


def _html_slug(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', value.lower()).strip('-')


def _html_table(lines: list[str]) -> str:
    rows = [[cell.strip() for cell in line.strip().strip('|').split('|')] for line in lines]
    if len(rows) < 2:
        return ""
    header, _, *body = rows
    html = ['<table><thead><tr>']
    for cell in header:
        html.append(f'<th>{_html_inline(cell)}</th>')
    html.append('</tr></thead><tbody>')
    for row in body:
        tput_nums = []
        for idx, cell in enumerate(row[1:], 1):
            match = re.search(r'^([\d.]+)\s*MiB/s', cell)
            if match:
                tput_nums.append((float(match.group(1)), idx))
        time_nums = []
        for idx, cell in enumerate(row[1:], 1):
            match = re.search(r'^([\d.]+)s(?:\s|$)', cell.strip())
            if match:
                time_nums.append((float(match.group(1)), idx))
        win_idx = (
            max(tput_nums, key=lambda item: item[0])[1] if tput_nums
            else min(time_nums, key=lambda item: item[0])[1] if time_nums
            else -1
        )
        html.append('<tr>')
        for idx, cell in enumerate(row):
            css_class = ' class="win"' if idx == win_idx else ''
            if cell in ('—', '*not available*') or 'not present' in cell.lower():
                css_class = ' class="na"'
            html.append(f'<td{css_class}>{_html_inline(cell)}</td>')
        html.append('</tr>')
    html.append('</tbody></table>')
    return ''.join(html)


def _html_code_block(language: str, code: str) -> str:
    if language == "mermaid":
        return '<div class="diagram-card"><div class="mermaid">' + code.strip() + '</div></div>'
    class_attr = f' class="language-{_html_esc(language)}"' if language else ''
    return f'<pre><code{class_attr}>{_html_esc(code)}</code></pre>'


def render_html(md_text: str) -> str:
    """Convert a rendered benchmark Markdown report to a styled HTML page."""
    html_css = _report_asset_text(HTML_CSS_PATH)
    html_script = _report_asset_text(HTML_SCRIPT_PATH)
    lines = md_text.splitlines()
    sections: list[tuple[str, str, bool]] = []
    body: list[str] = []
    h1_text = ''
    subtitle_text = ''
    uses_mermaid = False
    idx = 0
    used_anchors: dict[str, int] = {}

    def _unique_anchor(text: str) -> str:
        # Repeated per-study sections reuse headings; keep ids unique.
        anchor = _html_slug(text)
        count = used_anchors.get(anchor, 0) + 1
        used_anchors[anchor] = count
        return anchor if count == 1 else f'{anchor}-{count}'

    while idx < len(lines):
        line = lines[idx]
        if line.startswith('# ') and not line.startswith('## '):
            h1_text = line[2:].strip()
            idx += 1
            continue
        if line.startswith('## '):
            text = line[3:].strip()
            anchor = _unique_anchor(text)
            sections.append((anchor, text, False))
            body.append(f'<h2 id="{anchor}">{_html_inline(text)}</h2>')
            idx += 1
            continue
        if line.startswith('### '):
            text = line[4:].strip()
            anchor = _unique_anchor(text)
            sections.append((anchor, text, True))
            body.append(f'<h3 id="{anchor}">{_html_inline(text)}</h3>')
            idx += 1
            continue
        if line.startswith('_') and 'Generated from' in line and not subtitle_text:
            subtitle_text = _html_inline(line.strip('_').strip())
            idx += 1
            continue
        if line.startswith('```'):
            language = line[3:].strip().lower()
            idx += 1
            code_lines: list[str] = []
            while idx < len(lines) and not lines[idx].startswith('```'):
                code_lines.append(lines[idx])
                idx += 1
            if idx < len(lines) and lines[idx].startswith('```'):
                idx += 1
            if language == 'mermaid':
                uses_mermaid = True
            body.append(_html_code_block(language, '\n'.join(code_lines)))
            continue
        if line.startswith('|'):
            table_lines = []
            while idx < len(lines) and lines[idx].startswith('|'):
                table_lines.append(lines[idx])
                idx += 1
            body.append(_html_table(table_lines))
            continue
        if line.startswith('- '):
            items = []
            while idx < len(lines) and lines[idx].startswith('- '):
                items.append(f'<li>{_html_inline(lines[idx][2:])}</li>')
                idx += 1
            body.append('<ul>' + ''.join(items) + '</ul>')
            continue
        if not line.strip():
            idx += 1
            continue
        body.append(f'<p>{_html_inline(line)}</p>')
        idx += 1

    toc = ['<nav class="toc"><h2>Contents</h2>']
    for anchor, label, is_sub in sections:
        css_class = ' class="sub"' if is_sub else ''
        toc.append(f'<a href="#{anchor}"{css_class}>{_html_esc(label)}</a>')
    toc.append('</nav>')

    mermaid_script = (
        '<script type="module">\n'
        'import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";\n'
        'mermaid.initialize({startOnLoad:true,theme:"dark",securityLevel:"loose"});\n'
        '</script>\n'
        if uses_mermaid else ''
    )

    return (
        f'<!DOCTYPE html>\n<html lang="en"><head><meta charset="utf-8">\n'
        f'<meta name="viewport" content="width=device-width,initial-scale=1">\n'
        f'<title>Benchmark Report &mdash; nw-data-benchmarks</title>\n'
        f'<style>{html_css}</style></head>\n'
        f'<body><div class="layout">\n'
        f'{"".join(toc)}\n'
        f'<div class="page">\n'
        f'<h1>{_html_esc(h1_text)}</h1>\n'
        f'<p class="subtitle">{subtitle_text}</p>\n'
        f'{"".join(body)}\n'
        f'</div></div>\n'
        f'{mermaid_script}'
        f'<script>{html_script}</script>\n'
        f'</body></html>'
    )


__all__ = ["render_html"]

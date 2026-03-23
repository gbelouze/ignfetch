# AGENTS_HISTORY.md

This file tracks the history of agentic interventions and major changes to the repository.

## Progress Bar Enhancement (2026-03-23)

**Summary**: Implemented a flexible progress bar system to support both count-based and byte-based progress tracking within a single Progress instance.

**Changes**:
- Created `MofNMaybeBytes` custom column in `utils.py` that conditionally renders as M/N count or MB/MB based on `is_byte` field
- Updated `download_file()` in `main.py` to use native `advance()/total` instead of custom `completed_bytes`/`total_bytes` fields
- Removed `MofNCompleteColumn` import since functionality is now in `MofNMaybeBytes`
- All standard Rich columns (`BarColumn`, `TimeRemainingColumn`) now work natively without customization

**Context**: The challenge was displaying progress bars with different column formats for different tasks (overall download count vs per-file byte progress) while keeping all Rich columns working correctly for ETA and speed calculations.

**Note**: Also clarified AGENTS.md to emphasize always using `uv run` for Python execution.

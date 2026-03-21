from __future__ import annotations

import numpy as np


BIPOLAR_PAIRS = [
    ("Fp1", "F7"), ("F7", "T3"), ("T3", "T5"), ("T5", "O1"),
    ("Fp2", "F8"), ("F8", "T4"), ("T4", "T6"), ("T6", "O2"),
    ("Fp1", "F3"), ("F3", "C3"), ("C3", "P3"), ("P3", "O1"),
    ("Fp2", "F4"), ("F4", "C4"), ("C4", "P4"), ("P4", "O2"),
    ("Fz", "Cz"), ("Cz", "Pz"),
]

CHANNELS_10_20 = [
    "Fp1", "Fp2", "F7", "F3", "Fz", "F4", "F8",
    "T3", "C3", "Cz", "C4", "T4",
    "T5", "P3", "Pz", "P4", "T6",
    "O1", "O2",
]


def _apply_bipolar_montage(matrix: np.ndarray, labels: list[str]) -> np.ndarray:
    """Apply bipolar montage: each derived channel = ch_A - ch_B."""
    label_idx = {lbl: i for i, lbl in enumerate(labels)}
    derived = []
    for a, b in BIPOLAR_PAIRS:
        if a in label_idx and b in label_idx:
            derived.append(matrix[label_idx[a]] - matrix[label_idx[b]])
    if not derived:
        return matrix
    return np.vstack(derived)


def _build_sos(sample_freq: float) -> np.ndarray:
    """Build cascaded SOS filter: 60 Hz notch + 0.1-70 Hz bandpass."""
    from scipy.signal import butter, iirnotch, tf2sos

    b_notch, a_notch = iirnotch(60.0, 30.0, sample_freq)
    sos_notch = tf2sos(b_notch, a_notch)
    sos_bp = butter(4, [0.1, 70.0], btype="bandpass", fs=sample_freq, output="sos")
    return np.vstack([sos_notch, sos_bp])


def _apply_filters(matrix: np.ndarray, sos: np.ndarray) -> np.ndarray:
    from scipy.signal import sosfilt

    return sosfilt(sos, matrix, axis=1).astype(np.float32, copy=False)

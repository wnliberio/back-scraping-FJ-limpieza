# Este módulo “duplica” stdout/stderr a LOG_FILE.


# core/utils/tee.py
import sys
from datetime import datetime
from ..config import LOG_FILE

class _TeeStream:
    def __init__(self, original_stream, log_file_handle):
        self._original = original_stream
        self._file = log_file_handle

    def write(self, data):
        # Escribimos en ambos destinos
        try:
            self._original.write(data)
        except Exception:
            pass
        try:
            self._file.write(data)
        except Exception:
            pass

    def flush(self):
        try:
            self._original.flush()
        except Exception:
            pass
        try:
            self._file.flush()
        except Exception:
            pass

    # Compatibilidad con algunas librerías
    def isatty(self):
        try:
            return self._original.isatty()
        except Exception:
            return False

    def fileno(self):
        try:
            return self._original.fileno()
        except Exception:
            raise OSError("No fileno")

# Estado global simple
_ORIG_STDOUT = None
_ORIG_STDERR = None
_LOG_HANDLE = None

def start_tee(append: bool = True):
    global _ORIG_STDOUT, _ORIG_STDERR, _LOG_HANDLE
    if _LOG_HANDLE is not None:
        return  # ya activado

    mode = "a" if append else "w"
    _LOG_HANDLE = open(LOG_FILE, mode, encoding="utf-8", buffering=1)  # line-buffered

    # Cabecera de sesión
    header = f"\n========== RUN START {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ==========\n"
    _LOG_HANDLE.write(header)
    _LOG_HANDLE.flush()

    _ORIG_STDOUT = sys.stdout
    _ORIG_STDERR = sys.stderr

    sys.stdout = _TeeStream(_ORIG_STDOUT, _LOG_HANDLE)
    sys.stderr = _TeeStream(_ORIG_STDERR, _LOG_HANDLE)

def stop_tee():
    global _ORIG_STDOUT, _ORIG_STDERR, _LOG_HANDLE
    if _LOG_HANDLE is None:
        return

    # Restaurar
    if _ORIG_STDOUT is not None:
        sys.stdout = _ORIG_STDOUT
    if _ORIG_STDERR is not None:
        sys.stderr = _ORIG_STDERR

    try:
        _LOG_HANDLE.write(f"=========== RUN END {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===========\n")
        _LOG_HANDLE.flush()
        _LOG_HANDLE.close()
    except Exception:
        pass

    _ORIG_STDOUT = None
    _ORIG_STDERR = None
    _LOG_HANDLE = None

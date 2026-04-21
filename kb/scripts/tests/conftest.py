"""pytest config: put kb/scripts/ on sys.path so tests can import siblings."""

import sys
from pathlib import Path

SCRIPTS = Path(__file__).parent.parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

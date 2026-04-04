"""Enroll Schwab OAuth tokens. Run interactively."""
import sys

from schwabagent.config import Config
from schwabagent.schwab_client import SchwabClient

which = sys.argv[1] if len(sys.argv) > 1 else "both"
if which not in ("account", "market", "both"):
    print(f"Usage: python enroll.py [account|market|both]")
    sys.exit(1)

c = SchwabClient(Config())
c.enroll(which)

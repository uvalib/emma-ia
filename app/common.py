# app/common.py
#
# Common definitions.

from typing import Union, Dict, List, Optional, ItemsView, KeysView, ValuesView, Set

from app.util   import *
from app.var    import *
from app.output import *


# =============================================================================
# Constants
# =============================================================================


# Deployments.

DEPLOYMENTS    = ('production', 'staging')
DEF_DEPLOYMENT = DEPLOYMENTS[0]

# Member repositories.

REPO_TABLE = {
    'ia': 'archive',
    'ht': 'hathi',
    'bs': 'bookshare'
}

ALL_REPOS    = tuple(REPO_TABLE.keys())
TARGET_REPOS = to_tuple('ia')
DEF_REPO     = TARGET_REPOS[0]



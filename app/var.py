# app/var.py
#
# Environment variable values.


import os

from app.util import is_true


# =============================================================================
# Environment variables
# =============================================================================

DRY_RUN = is_true(os.getenv('DRY_RUN'))

if os.getenv('DEBUG'):
    DEBUG      = is_true(os.getenv('DEBUG'))
    AWS_DEBUG  = is_true(os.getenv('AWS_DEBUG',  DEBUG))
    EMMA_DEBUG = is_true(os.getenv('EMMA_DEBUG', DEBUG))
    IA_DEBUG   = is_true(os.getenv('IA_DEBUG',   DEBUG))
else:
    AWS_DEBUG  = is_true(os.getenv('AWS_DEBUG',  True))
    EMMA_DEBUG = is_true(os.getenv('EMMA_DEBUG', True))
    IA_DEBUG   = is_true(os.getenv('IA_DEBUG',   True))
    DEBUG      = DRY_RUN or AWS_DEBUG or EMMA_DEBUG or IA_DEBUG

APPLICATION_DEPLOYED = not not os.getenv('AWS_REGION')

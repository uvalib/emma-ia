# app/aws.py
#
# AWS interface definitions.


from app.aws_s3     import *
from app.aws_sqs    import *
from app.aws_lambda import *


# =============================================================================
# Command-line tests.
# =============================================================================


if __name__ == '__main__':
    from tests.aws import trials
    trials()

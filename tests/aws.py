# tests/aws.py
#
# AWS interface trials.

from app.common import *

from tests.aws_s3     import trials as s3_trials
from tests.aws_sqs    import trials as sqs_trials
from tests.aws_lambda import trials as lambda_trials


# =============================================================================
# Trials
# =============================================================================


def trials():
    show_section('AWS TRIALS')
    s3_trials()
    sqs_trials(send_msgs=False)
    lambda_trials()
    show_section()


if __name__ == '__main__':
    trials()

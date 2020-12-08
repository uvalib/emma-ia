# app/aws_lambda.py
#
# AWS Lambda interface definitions.


import boto3
import boto3_type_annotations.lambda_ as lam

from app.common import *


# =============================================================================
# Constants
# =============================================================================


# @see https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html
LAMBDA_RUNTIMES = [
    'nodejs12.x',
    'nodejs10.x',
    'python3.8',
    'python3.7',
    'python3.6',
    'python2.7',
    'ruby2.7',
    'ruby2.5',
    'java11',
    'java8.a12',
    'java8',
    'go1.x',
    'dotnetcore3.1',
    'dotnetcore2.1',
    'provided.al2',     # Custom runtime - Amazon Linux 2
    'provided',         # Custom runtime - Amazon Linux
]


# =============================================================================
# AWS Lambda
# =============================================================================


def get_lambda_client() -> lam.Client:
    return boto3.client('lambda')


def get_lambda_functions(cli=None, all_versions=False) -> List[Dict]:
    """
    :param lam.Client cli:
    :param bool       all_versions:
    """
    cli = cli or get_lambda_client()
    kwargs = {}
    if all_versions:
        kwargs['FunctionVersion'] = 'ALL'
    result = cli.list_functions(**kwargs)
    result = result.get('Functions', ['FAIL'])
    return result


def get_lambda_function_names(cli=None) -> List[str]:
    """
    :param lam.Client cli:
    """
    result = []
    for entry in get_lambda_functions(cli):
        name = entry.get('FunctionName')
        if name:
            result.append(name)
    return result

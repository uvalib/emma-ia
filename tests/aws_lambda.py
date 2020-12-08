# tests/aws_lambda.py
#
# AWS Lambda interface trials.


from app.aws_lambda import *


# =============================================================================
# Functions
# =============================================================================


def show_lambda_functions(cli=None, all_versions=False):
    """
    :param lam.Client cli:
    :param bool       all_versions:
    """
    show(get_lambda_functions(cli, all_versions))


def show_lambda_layers(cli=None, runtime=None):
    """
    :param lam.Client cli:
    :param str        runtime:          One of LAMBDA_RUNTIMES
    """
    cli = cli or get_lambda_client()
    kwargs = {}
    if runtime:
        kwargs['CompatibleRuntime'] = runtime
    result = cli.list_layers(**kwargs)
    result = result.get('Layers', ['FAIL'])
    show(result)


def show_lambda_mappings(cli=None, function=None):
    """
    :param lam.Client cli:
    :param str        function:     If not specified, all functions are shown.
    """
    cli = cli or get_lambda_client()
    result = {}
    if function:
        names = [function]
    else:
        names = get_lambda_function_names(cli)
    for name in names:
        response = cli.list_event_source_mappings(FunctionName=name)
        result[name] = response.get('EventSourceMappings', ['FAIL'])
    show(result)


# =============================================================================
# Trials
# =============================================================================


def trials():
    cli = get_lambda_client()

    if True:
        show_header('Lambda functions')
        show(get_lambda_function_names(cli))
        show(get_lambda_functions(cli))

    if True:
        show_header('Lambda layers')
        show_lambda_layers(cli)

    if True:
        show_header('Lambda event source mappings')
        show_lambda_mappings(cli)


if __name__ == '__main__':
    trials()

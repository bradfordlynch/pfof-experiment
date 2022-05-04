import json

import boto3


def get_secret(id_secret):
    """
    Gets a secret from AWS
    """
    client_secrets = boto3.client("secretsmanager")
    resp = client_secrets.get_secret_value(SecretId=id_secret)

    assert (
        resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ), "Secrets get failed with non-200 status"

    return json.loads(resp["SecretString"])


def put_secret(id_secret, secret):
    """
    Puts a secret in AWS
    """
    client_secrets = boto3.client("secretsmanager")
    resp = client_secrets.put_secret_value(
        SecretId=id_secret, SecretString=json.dumps(secret)
    )

    assert (
        resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ), "Secrets put failed with non-200 status"

    return True


class PrintLogger:
    """
    Hack to print shit
    """

    def __init__(self) -> None:
        pass

    def debug(self, output):
        self._print("DEBUG", output)

    def info(self, output):
        self._print("INFO", output)

    def warn(self, output):
        self._print("WARN", output)

    def error(self, output):
        self._print("ERROR", output)

    def _print(self, level, output):
        print(f"{level} - {output}")

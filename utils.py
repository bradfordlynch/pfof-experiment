import json

import boto3


def get_secret(id_secret):
    client_secrets = boto3.client("secretsmanager")
    resp = client_secrets.get_secret_value(SecretId=id_secret)

    assert (
        resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ), "Secrets get failed with non-200 status"

    return json.loads(resp["SecretString"])


def put_secret(id_secret, secret):
    client_secrets = boto3.client("secretsmanager")
    resp = client_secrets.put_secret_value(
        SecretId=id_secret, SecretString=json.dumps(secret)
    )

    assert (
        resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    ), "Secrets put failed with non-200 status"

    return True

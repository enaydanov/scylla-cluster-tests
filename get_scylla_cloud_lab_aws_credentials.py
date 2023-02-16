import json

from sdcm.keystore import KeyStore


def scylla_cloud_lab_aws_credentials() -> str:
    creds = KeyStore().get_cloud_access()
    return json.dumps({
        "Version": 1,
        "AccessKeyId": creds["aws_access_key_id"],
        "SecretAccessKey": creds["aws_secret_access_key"],
    })


if __name__ == "__main__":
    print(scylla_cloud_lab_aws_credentials())

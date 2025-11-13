import os
from time import sleep

import paramiko
import pytest
from testcontainers.sftp import SFTPContainer, SFTPUser
from typer.testing import CliRunner

from datacontract.cli import app

sftp_dir = "/sftp/data"

csv_file_name = "sample_data"
csv_file_path = f"fixtures/csv/data/{csv_file_name}.csv"
csv_sftp_path = f"{sftp_dir}/{csv_file_name}.csv"

parquet_file_name = "combined_no_time"
parquet_file_path = f"fixtures/parquet/data/{parquet_file_name}.parquet"
parquet_sftp_path = f"{sftp_dir}/{parquet_file_name}.parquet"

avro_file_name = "orders"
avro_file_path = f"fixtures/avro/data/{avro_file_name}.avsc"
avro_sftp_path = f"{sftp_dir}/{avro_file_name}.avsc"

dbml_file_name = "dbml"
dbml_file_path = f"fixtures/dbml/import/{dbml_file_name}.txt"
dbml_sftp_path = f"{sftp_dir}/{dbml_file_name}.txt"

dbt_file_name = "manifest_jaffle_duckdb"
dbt_file_path = f"fixtures/dbt/import/{dbt_file_name}.json"
dbt_sftp_path = f"{sftp_dir}/{dbt_file_name}.json"

iceberg_file_name = "simple_schema"
iceberg_file_path = f"fixtures/iceberg/{iceberg_file_name}.json"
iceberg_sftp_path = f"{sftp_dir}/{iceberg_file_name}.json"

json_file_name = "orders"
json_file_path = f"fixtures/import/{json_file_name}.json"
json_sftp_path = f"{sftp_dir}/{json_file_name}.json"

odcs_file_name = "full-example"
odcs_file_path = f"fixtures/odcs_v3/{odcs_file_name}.odcs.yaml"
odcs_sftp_path = f"{sftp_dir}/{odcs_file_name}.odcs.yaml"

username = "demo"  # for emberstack
password = "demo"  # for emberstack
user = SFTPUser(name = username,password=password)


@pytest.fixture(params=[{'filepath': csv_file_path, 'sftp_path': csv_sftp_path,'filetype': 'csv'},
                        {'filepath': parquet_file_path, 'sftp_path': parquet_sftp_path, 'filetype': 'parquet'},
                        {'filepath': avro_file_path, 'sftp_path': avro_sftp_path, 'filetype': 'avro'},
                        {'filepath': dbml_file_path, 'sftp_path': dbml_sftp_path, 'filetype': 'dbml'},
                        {'filepath': dbt_file_path, 'sftp_path': dbt_sftp_path,'filetype': 'dbt'},
                        {'filepath': iceberg_file_path, 'sftp_path': iceberg_sftp_path, 'filetype': 'iceberg'},
                        {'filepath': json_file_path, 'sftp_path': json_sftp_path, 'filetype': 'jsonschema'},
                        {'filepath': odcs_file_path, 'sftp_path': odcs_sftp_path, 'filetype': 'odcs'},
                       ])
def sftp_container(request):
    """
    Initialize and provide an SFTP container for all tests in this module.
    Sets up the container, uploads the test file, and provides connection details.
    """
    # that image is both compatible with Mac and Linux which is not the case with the default image
    with SFTPContainer(image="emberstack/sftp:latest", users=[user]) as container:
        host_ip = container.get_container_host_ip()
        host_port = container.get_exposed_sftp_port()

        # Set environment variables for SFTP authentication
        os.environ["DATACONTRACT_SFTP_USER"] = username
        os.environ["DATACONTRACT_SFTP_PASSWORD"] = password

        # Wait for the container to be ready
        sleep(3)

        # Upload test files to SFTP server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host_ip, host_port, username, password)
        sftp = ssh.open_sftp()
        try:
            sftp.mkdir(sftp_dir)
            sftp.put(request.param['filepath'], request.param['sftp_path'])
            source = f"sftp://{host_ip}:{host_port}{csv_sftp_path}"

            runner = CliRunner()
            result = runner.invoke(
                app,
                [
                    "import",
                    "--format",
                    request.param['filetype'],
                    "--source",
                    source,
                ],
            )
            assert result.exit_code == 0
        finally:
            sftp.close()
            ssh.close()


def test_cli(sftp_container):
    # The actual test is performed in the fixture. Each parameter set will run this test.
    pass
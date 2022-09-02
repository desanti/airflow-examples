from tempfile import NamedTemporaryFile
from typing import Sequence, Optional, Union, Any, Callable, Collection, Mapping

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import KeywordParameters


class S3PythonTransformOperator(BaseOperator):
    """
    Copies data from a source S3 location to a temporary location on the
    local filesystem. Runs a transformation on this file as specified by
    the transformation Python function and uploads the output to a destination S3
    location.

    :param source_aws_conn_id: source s3 connection
    :param source_s3_bucket: The specific bucket to use to source
    :param source_s3_key: The key to be retrieved from S3. (templated)
    :param source_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
             (unless use_ssl is False), but SSL certificates will not be
             verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
             You can specify this argument if you want to use a different
             CA cert bundle than the one used by botocore.

        This is also applicable to ``dest_verify``.
    :param dest_aws_conn_id: destination s3 connection
    :param dest_s3_bucket: The specific bucket to use to destination
    :param dest_s3_key: The key to be written from S3. (templated)
    :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
        See: ``source_verify``
    :param replace: Replace destination S3 key if it already exists
    :param python_callable: A reference to an object that is callable,
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param show_return_value_in_logs: a bool value whether to show return_value
        logs. Defaults to True, which allows return value log output.
        It can be set to False to prevent log output of return value when you return huge data
        such as transmission a large amount of XCom to TaskAPI.
    """

    template_fields: Sequence[str] = (
        "source_s3_key",
        "dest_s3_key",
        "op_args",
        "op_kwargs",
    )
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}
    template_ext: Sequence[str] = ()
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        source_aws_conn_id: str = "aws_default",
        source_s3_bucket: Optional[str] = None,
        source_s3_key: str,
        source_verify: Optional[Union[bool, str]] = None,
        dest_aws_conn_id: str = "aws_default",
        dest_s3_bucket: Optional[str] = None,
        dest_s3_key: str,
        dest_verify: Optional[Union[bool, str]] = None,
        replace: bool = False,
        python_callable: Callable,
        op_args: Optional[Collection[Any]] = None,
        op_kwargs: Optional[Mapping[str, Any]] = None,
        show_return_value_in_logs: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_s3_bucket = source_s3_bucket
        self.source_s3_key = source_s3_key
        self.source_aws_conn_id = source_aws_conn_id
        self.source_verify = source_verify
        self.dest_s3_bucket = dest_s3_bucket
        self.dest_s3_key = dest_s3_key
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.replace = replace
        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}
        self.show_return_value_in_logs = show_return_value_in_logs

    def execute(self, context: Context) -> Any:
        with NamedTemporaryFile(
            "wb", prefix="airflow_s3_"
        ) as file_source, NamedTemporaryFile("wb", prefix="airflow_s3_") as file_dest:

            self.log.info("Downloading source S3 file %s", self.source_s3_key)

            self._download_file(file_source)

            context_merge(
                context,
                self.op_kwargs,
                input_filename=file_source.name,
                output_filename=file_dest.name,
            )
            self.op_kwargs = self._determine_kwargs(context)

            self.log.info("Executing transform function")
            return_value = self._execute_callable()

            self.log.info("Uploading transformed file to S3")
            file_dest.flush()

            self._upload_file(file_dest.name)

            self.log.info("Upload successful")

            if self.show_return_value_in_logs:
                self.log.info("Done. Returned value was: %s", return_value)
            else:
                self.log.info("Done. Returned value not shown")

    def _determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        return KeywordParameters.determine(
            self.python_callable, self.op_args, context
        ).unpacking()

    def _execute_callable(self):
        """
        Calls the python callable with the given arguments.

        :return: the return value of the call.
        :rtype: any
        """
        return self.python_callable(*self.op_args, **self.op_kwargs)

    def _download_file(self, fileobject):
        """
        Downloads a file from the S3 location to the local file system.

        :param fileobject: Temporary file object
        :return:
        """
        conn = S3Hook(aws_conn_id=self.source_aws_conn_id, verify=self.source_verify)

        if not conn.check_for_key(self.source_s3_key, self.source_s3_bucket):
            raise AirflowException(
                f"The source key {self.source_s3_key} does not exist"
            )

        s3_key = conn.get_key(self.source_s3_key, self.source_s3_bucket)
        s3_key.download_fileobj(Fileobj=fileobject)

    def _upload_file(self, local_filename: str):
        """
        Upload local file to S3 destination

        :param local_filename: temp local filename
        """
        conn = S3Hook(aws_conn_id=self.dest_aws_conn_id, verify=self.dest_verify)

        conn.load_file(
            filename=local_filename,
            key=self.dest_s3_key,
            bucket_name=self.dest_s3_bucket,
            replace=True,
        )

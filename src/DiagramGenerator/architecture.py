from diagrams import Diagram, Node
from diagrams.aws.analytics import Glue
from diagrams.programming.language import Python
# from diagrams.aws.compute import Lambda
# from diagrams.aws.analytics import KinesisDataFirehose
from diagrams.aws.storage import S3
from diagrams.saas.analytics import Snowflake
from diagrams.aws.security import IdentityAndAccessManagementIamAddOn
from diagrams.aws.general import GenericDatabase
from diagrams.onprem.analytics import Dbt



with Diagram("AWS-glue-dbt-snowflake pipeline", show=False, filename="diagrams/architecture", outformat="png"):
    python = Python("Extract data from an external API")
    glue = Glue("AWS Glue")
    iam = IdentityAndAccessManagementIamAddOn("IAM Role")
    iam2 = IdentityAndAccessManagementIamAddOn("IAM Role")
    raw = GenericDatabase("raw")
    transform = GenericDatabase("transform")
    mart = GenericDatabase("mart")
    # lam = Lambda("Lambda Processor")
    # firehose = KinesisDataFirehose("Firehose")
    s3 = S3("S3 Bucket")
    # s3error = S3("Error Logs")
    snowflake = Snowflake("GLUEDB_PRODUCTION")
    dbt = Dbt("dbt managed")

    glue >> python >> iam >> s3 >> iam2 >> snowflake >> dbt >> raw >> transform >> mart
    
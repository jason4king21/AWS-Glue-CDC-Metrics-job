# architecture_diagram.py

from diagrams import Diagram, Cluster
from diagrams.aws.database import RDS
from diagrams.aws.analytics import Glue
from diagrams.aws.storage import S3
from diagrams.aws.compute import EC2
from diagrams.aws.management import Cloudwatch
from diagrams.custom import Custom
from urllib.request import urlretrieve

# Download GitHub Actions icon
# gha_icon_url = "https://techicons.dev/icons/githubactions/github actions.svg"
gha_icon = "GitHub_Actions.png"
# urlretrieve(gha_icon_url, gha_icon)

with Diagram("AWS Glue CDC & Metrics Pipeline", show=False,  direction="LR"):
    # Source
    rds = RDS("RDS SQL Server")
    
    # Ingestion & ETL
    with Cluster("AWS Glue (PySpark CDC)"):
        glue = Glue("CDC & Metrics Job")
    
    # Storage zones
    with Cluster("Amazon S3"):
        bronze = S3("Bronze")
        silver = S3("Silver")
        gold = S3("Gold")
    
    # Visualizations
    dashboard = EC2("Streamlit Dashboard")
    
    # Monitoring
    log = Cloudwatch("Glue Logs")
    
    # CI/CD
    gha = Custom("GitHub Actions", "./github.png")
    
    # Data flow
    gha >> glue  # CI/CD triggers updated job
    gha >> dashboard  # Deploys Streamlit updates
    rds >> glue
    glue >> log
    glue >> bronze >> silver >> gold
    gold >> dashboard

Write-Host "Syncing dbt project to EC2..."

scp -r dbt/fmcg_pipeline/models fmcg-ec2:/home/ec2-user/dbt/fmcg_pipeline/
scp -r dbt/fmcg_pipeline/snapshots fmcg-ec2:/home/ec2-user/dbt/fmcg_pipeline/
scp -r dbt/fmcg_pipeline/macros fmcg-ec2:/home/ec2-user/dbt/fmcg_pipeline/
scp dbt/fmcg_pipeline/dbt_project.yml fmcg-ec2:/home/ec2-user/dbt/fmcg_pipeline/

Write-Host "Sync complete!"
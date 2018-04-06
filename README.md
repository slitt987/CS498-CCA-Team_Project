# CS498-CCA-Team_Project
CS498-CCA-Team_Project


**Collection**
Python Packages Required:
 - boto3
 - bs4
 - lxml
 - requests
 - elasticsearch

Assumed you have configured AWS CLI using 
`aws configure`

Your IAM role will need privileges to EC2 across zones.  If you use a root or admin account this is fine or you can use this policy
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1470583998000",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeAvailabilityZones",
                "ec2:DescribeRegions",
                "ec2:DescribeSpotPriceHistory"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
```

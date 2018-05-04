# CS498-CCA-Team_Project
CS498-CCA-Team_Project


**Collection**
Python Packages Required:
 - boto3
 - bs4
 - lxml
 - requests
 - elasticsearch
 - flask (for REST API)

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

create a self signed cert in the directory called cert.pem with key key.pem
   - openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

Example API usage (-k for self signed cert):
curl -d '{"Region" : "us-east-1"}' -H "Content-Type: application/json" -X POST -k https://localhost:5000/get_bid/1
curl -d '{"Region" : "us-east-1", "vcpu": [4, 8]}' -H "Content-Type: application/json" -X POST -k https://localhost:5000/get_bid/1

Special request fields (optional):
 - timestamp - when you will place the bid - default now
 - numeric_as_min - should numeric arguments be treated as lower bounds - default True

Available search fields:
 - InstanceType : keyword
 - Region : keyword
 - capacitystatus : keyword
 - clockSpeed : keyword
 - currentGeneration : keyword
 - dedicatedEbsThroughput : keyword
 - driveQuantity : long
 - driveSize : float
 - ebsOptimized : keyword
 - ecu : float
 - enhancedNetworkingSupported : keyword
 - gpu : keyword
 - instanceFamily : keyword
 - intelAvx2Available : keyword
 - intelAvxAvailable : keyword
 - intelTurboAvailable : keyword
 - licenseModel : keyword
 - memorySize : float
 - memorySizeUnits : keyword
 - networkPerformance : keyword
 - normalizationSizeFactor : long
 - physicalCores : keyword
 - physicalProcessor : keyword
 - processorArchitecture : keyword
 - processorFeatures : keyword
 - storageType : keyword
 - tenancy : keyword
 - vcpu : long


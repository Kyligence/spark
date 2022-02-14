#!/bin/bash
echo "Compressing..."
cd _book/
tar -zcvf /home/jenkins/enterprise_v4.5.tar.gz * > /dev/null
cd ..
echo "Uploading..."
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r /home/jenkins/enterprise_v4.5.tar.gz ec2-user@52.82.19.185:/home/ec2-user/
ssh -o StrictHostKeyChecking=no ec2-user@52.82.19.185 "tar -zxvf /home/ec2-user/enterprise_v4.5.tar.gz -C /opt/books/v4.5/ > /dev/null"
ssh -o StrictHostKeyChecking=no ec2-user@52.82.19.185 "rm /home/ec2-user/enterprise_v4.5.tar.gz"
rm -f /home/jenkins/enterprise_v4.5.tar.gz
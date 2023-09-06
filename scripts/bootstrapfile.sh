#!/bin/bash
# Script para instalar a biblioteca boto3 no cluster EMR

# Atualize o sistema
sudo yum -y update

# Instale o pip (gerenciador de pacotes Python)
sudo yum -y install python3-pip

# Instale o boto3 usando o pip
sudo python3 -m pip install boto3

sudo wget -P /usr/lib/spark/jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0-all.jar
## MS SQL Server setup on a Linux machine

Followed instructions at https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-ubuntu?view=sql-server-linux-ver15.

Chose edition 1.

Add directory to the path of every user.
```
sudo su
echo "export PATH="$PATH:/opt/mssql-tools/bin"" >> /etc/profile
Ctrl+D
```

Create login for database server.
```sql
CREATE LOGIN bokanyie WITH PASSWORD = 'AppleTree1234'
GO
```

Check status of server, restart server
```bash
sudo systemctl status mssql-server
sudo systemctl restart mssql-server
```

HDD for MSSQL storage

```bash
sudo mkfs -t ext4 -j /dev/vdc
mkdir /mnt/mssql
sudo mount /dev/vdc /mnt/mssql
sudo chmod ugo+rwx /mnt/mssql
```

Connecting to MSSQL Server from VSCode (if does not work, check if server is alive!):
```
hostname\instance: 193.224.59.119,1433 # 1433 is the default port for the MSSQL Server, opened this port in the rules!
username: bokanyie # choose SQL login
password: AppleTree1234
```

Installing `pymssql`
```bash
pip install pymssql
```

## Getting the dataset

Migration of gulls: https://www.gbif.org/dataset/83e20573-f7dd-4852-9159-21566e1e691e

GBIF.org (26 January 2022) GBIF Occurrence Download  https://doi.org/10.15468/dl.emyv5v


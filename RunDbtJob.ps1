# Define the remote VM details
$remoteVM = "PC-VM-A9-65-CD.preferredcredit.net"
$remoteUser = "PREFERREDCREDIT\lda_pokr"
$remotePassword = "passwordstring" # Use a secure method to handle passwords
$customPort = 5986
$target = "dev"

# Create a PSCredential object
$securePassword = ConvertTo-SecureString $remotePassword -AsPlainText -Force
$credential = New-Object System.Management.Automation.PSCredential ($remoteUser, $securePassword)

# Define the script to run on the remote VM
$remoteScript = {
    cd "C:\Program Files\dbt\pic_dbt_project"
    dbt run -t "dev"
}

# Use Invoke-Command to run the script on the remote VM using the custom port
Invoke-Command -ComputerName $remoteVM -Credential $credential -ScriptBlock $remoteScript -Port $customPort  -UseSSL

exit
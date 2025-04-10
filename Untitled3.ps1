# Define Kafka Connect (KC) Environments with Credentials
$KC_Environments = @(
    @{ Name = "EKS"; Url = "https://eks-kafka-connect-url"; ClientID = "eks-client"; ClientSecret = "eks-secret"; Resource = "eks-resource" },
    @{ Name = "ECS"; Url = "https://ecs-kafka-connect-url"; ClientID = "ecs-client"; ClientSecret = "ecs-secret"; Resource = "ecs-resource" }
)

# Define Hardcoded Values for Schema Registry
$NewSchemaRegistryUrl = "https://your-new-schema-registry-url"

# Error Tracking List
$ErrorList = @()

# Loop Through Each KC Environment
foreach ($KC in $KC_Environments) {
    Write-Host "Processing KC: $($KC.Name)"

    # Request OAuth 2.0 Token
    try {
        Write-Host "Requesting OAuth token for $($KC.Name)..."
        $TokenResponse = Invoke-RestMethod -Uri "https://your-oauth-token-url" -Method Post -Body @{
            grant_type    = "client_credentials"
            client_id     = $KC.ClientID
            client_secret = $KC.ClientSecret
            resource      = $KC.Resource
        } -ContentType "application/x-www-form-urlencoded"
        
        $AccessToken = $TokenResponse.access_token
        Write-Host "OAuth token received for $($KC.Name)."
    } catch {
        $ErrorList += [PSCustomObject]@{ KC = $KC.Name; Connector = ""; Errors = "OAuth Token Error: $_" }
        Write-Host "ERROR: Failed to get OAuth token for $($KC.Name) - $_"
        continue
    }

    # Authorization Header
    $Headers = @{
        Authorization  = "Bearer $AccessToken"
        "Content-Type" = "application/json"
    }

    # Get List of Connectors
    try {
        Write-Host "Fetching connectors from $($KC.Name)..."
        $ConnectorNames = Invoke-RestMethod -Uri "$($KC.Url)/connectors" -Method Get -Headers $Headers
        Write-Host "Found $($ConnectorNames.Count) connectors in $($KC.Name)."
    } catch {
        $ErrorList += [PSCustomObject]@{ KC = $KC.Name; Connector = ""; Errors = "Failed to Fetch Connectors: $_" }
        Write-Host "ERROR: Failed to fetch connectors from $($KC.Name) - $_"
        continue
    }

    # Loop Through Each Connector
    foreach ($Connector in $ConnectorNames) {
        $ConfigUrl = "$($KC.Url)/connectors/$Connector/config"

        try {
            # Fetch Connector Config
            $Config = Invoke-RestMethod -Uri $ConfigUrl -Method Get -Headers $Headers
        } catch {
            $ErrorList += [PSCustomObject]@{ KC = $KC.Name; Connector = $Connector; Errors = "Failed to Fetch Config: $_" }
            Write-Host "ERROR: Failed to fetch config for $Connector in $($KC.Name) - $_"
            continue
        }

        # 🔹 PROCESS ONLY IF CONNECTOR CLASS MATCHES 🔹
        if ($Config."connector.class" -eq "io.confluent.connect.aws.lambda.AwsLambdaSinkConnector") {
            Write-Host "Processing Connector: $Connector in $($KC.Name)"

            # Check if key.converter.schema.registry.url or value.converter.schema.registry.url Exists
            $Updated = $false

            try {
                if ($Config.PSObject.Properties["key.converter.schema.registry.url"]) {
                    Write-Host "Found key.converter.schema.registry.url: $($Config.'key.converter.schema.registry.url')"
                    $Config."key.converter.schema.registry.url" = $NewSchemaRegistryUrl
                    $Updated = $true
                }
            } catch {
                Write-Host "WARNING: key.converter.schema.registry.url not found in $Connector ($($KC.Name))"
            }

            try {
                if ($Config.PSObject.Properties["value.converter.schema.registry.url"]) {
                    Write-Host "Found value.converter.schema.registry.url: $($Config.'value.converter.schema.registry.url')"
                    $Config."value.converter.schema.registry.url" = $NewSchemaRegistryUrl
                    $Updated = $true
                }
            } catch {
                Write-Host "WARNING: value.converter.schema.registry.url not found in $Connector ($($KC.Name))"
            }

            # If Updates Were Made, Convert Config to JSON and Send Update Request
            if ($Updated) {
                $UpdatedConfigJson = $Config | ConvertTo-Json -Depth 10

                try {
                    Invoke-RestMethod -Uri $ConfigUrl -Method Put -Headers $Headers -Body $UpdatedConfigJson -ContentType "application/json"
                    Write-Host "Successfully updated $Connector in $($KC.Name)."
                } catch {
                    $ErrorList += [PSCustomObject]@{ KC = $KC.Name; Connector = $Connector; Errors = "Failed to Update Config: $_" }
                    Write-Host "ERROR: Failed to update config for $Connector in $($KC.Name) - $_"
                }
            }
        } else {
            Write-Host "Skipping Connector: $Connector in $($KC.Name) (Class does not match)"
        }
    }
}

# Export Errors to CSV
if ($ErrorList.Count -gt 0) {
    $ErrorList | Export-Csv -Path "Kafka_Connect_Errors.csv" -NoTypeInformation
    Write-Host "Errors saved to Kafka_Connect_Errors.csv"
} else {
    Write-Host "No errors encountered!"
}

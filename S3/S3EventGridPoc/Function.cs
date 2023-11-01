using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Util;

using Amazon.CognitoIdentity;
using Amazon.CognitoIdentity.Model;

using System.Collections;

using Microsoft.Identity.Client;
using Kusto.Data;
using Kusto.Data.Net.Client;
using Kusto.Ingest;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace S3EventGridPoc;

public class Function
{
    private readonly IKustoIngestClient m_client;

    IAmazonS3 S3Client { get; set; }

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance.
    /// </summary>
    public Function()
    {
        //const string cognitoPoolId = "eu-north-1:80ae676e-949e-4ddb-82cd-a0edc8266fc9";
        // const string clientId= "757a0e90-ab7c-42ff-ac1d-8cf7f5d028d6";
        // const string tenantId = "d1d92357-0fd1-4f9d-853e-a2bf3d687614";
        string region = Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION");
        
        string cognitoPoolId =Environment.GetEnvironmentVariable("CognitoPoolId");
        string appId = Environment.GetEnvironmentVariable("AppId");
        string appKey = Environment.GetEnvironmentVariable("AppKey");
        string authority = Environment.GetEnvironmentVariable("AppTenant");
        string clusterUri = Environment.GetEnvironmentVariable("IngestionUri");

                // this is debug code very useful to see what is going on
        if (true){
            // enumerate environment variables
            var entries = Environment.GetEnvironmentVariables().Cast<DictionaryEntry>()
                .Select(x => KeyValuePair.Create((string)x.Key, (string)x.Value));
            var sortedEntries = entries.OrderBy(x => x.Key);

            foreach ( var  v in sortedEntries)
            {
                Console.WriteLine("env var : " + v.Key + " = " + v.Value);
            }
        }

        if (appId != null &&  cognitoPoolId != null) {
            Console.WriteLine("using cognito");
            Dictionary<string,string>  Logins= new Dictionary<string,string>();
            Logins["Azure"]="Adx";
            S3Client = new AmazonS3Client();
            var cognitoClient = new 	AmazonCognitoIdentityClient( 	 RegionEndpoint.GetBySystemName(region ));
            var response =  cognitoClient.GetOpenIdTokenForDeveloperIdentityAsync(new GetOpenIdTokenForDeveloperIdentityRequest()
            {
                IdentityPoolId = cognitoPoolId,
                Logins = Logins
            }
            ).Result;

            Console.WriteLine($"Initializing an ingest client for {clusterUri}");
            //        Console.WriteLine($"cognito client identity is {cognitoClient.}");

            Console.WriteLine ($" got a response from cognito with identity:  {response.IdentityId}");
            
            if (response.Token.Length > 0){    
                Console.WriteLine ($" resp {response.Token}");
                IConfidentialClientApplication app = ConfidentialClientApplicationBuilder.Create(appId)
                    .WithClientAssertion(response.Token)      
                    .WithAuthority(AzureCloudInstance.AzurePublic, authority)          
                    .Build();
                var appAccessToken= app.AcquireTokenForClient(new string[] { $"https://management.core.windows.net/.default" }).ExecuteAsync().Result;
                if (appAccessToken != null){
                    Console.WriteLine("AAD token is " + appAccessToken.AccessToken);
                    
                    Console.WriteLine("creating ingestion client with AAD Token as the authentication") ;               
                    var kustoConnectionStringBuilderDM = new KustoConnectionStringBuilder(clusterUri)
                        .WithAadApplicationTokenAuthentication(appAccessToken.AccessToken);
                    m_client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);
                    Console.WriteLine ("constructed client with conn string:" + kustoConnectionStringBuilderDM.ToString());
                }
        }
        else if (appId != null && appKey != null){
            Console.WriteLine("using AAD with appKey");
            var kustoConnectionStringBuilderDM =
                new KustoConnectionStringBuilder(clusterUri)
                .WithAadApplicationKeyAuthentication(appId, appKey, authority);
                        Console.WriteLine($"Initializing an ingest client for {clusterUri}");
            // Create an ingest client
            // Note, that creating a separate instance per ingestion operation is an anti-pattern.
            // IngestClient classes are thread-safe and intended for reuse
            m_client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);
        }
        else {
            Console.WriteLine("no authentication available, set environment variables appKey or cognitoPoolId");
            
        }

        }
    }

    /// <summary>
    /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
    /// </summary>
    /// <param name="s3Client"></param>
    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;

        string appId = Environment.GetEnvironmentVariable("AppId");
        string appKey = Environment.GetEnvironmentVariable("AppKey");
        string authority = Environment.GetEnvironmentVariable("AppTenant");
        string clusterUri = Environment.GetEnvironmentVariable("IngestionUri");
        Console.WriteLine($"Initializing an ingest client for {clusterUri}");
        /*
        var kustoConnectionStringBuilderDM =
            new KustoConnectionStringBuilder(clusterUri)
            .WithAadApplicationKeyAuthentication(appId, appKey, authority);

        Console.WriteLine($"Initializing an ingest client for {clusterUri}");
        // Create an ingest client
        // Note, that creating a separate instance per ingestion operation is an anti-pattern.
        // IngestClient classes are thread-safe and intended for reuse
        m_client = KustoIngestFactory.CreateQueuedIngestClient(kustoConnectionStringBuilderDM);
        */

    }

    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
    /// to respond to S3 notifications.
    /// </summary>
    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        Console.WriteLine($"start to ingest");
        
        var awsCredentials = Environment.GetEnvironmentVariable("AwsCredentials");
        string table = Environment.GetEnvironmentVariable("TargetTable");
        string database = Environment.GetEnvironmentVariable("TargetDatabase");
        if (evnt.Records != null ){
            foreach (var record in evnt.Records)
            {
                var s3 = record.S3;
                Console.WriteLine($"[{record.AwsRegion} - {record.EventTime}] Bucket = {s3.Bucket.Name}, Key = {s3.Object.Key}");

                // Ingest from blobs according to the required properties
                var kustoIngestionProperties = new KustoQueuedIngestionProperties(databaseName: database, tableName: table)
                {
                    FlushImmediately = true
                };

                var sourceOptions = new StorageSourceOptions() { DeleteSourceOnSuccess = false, Size = s3.Object.Size };
                var uri = $"https://{s3.Bucket.Name}.s3.{record.AwsRegion}.amazonaws.com/{s3.Object.Key}";
                Console.WriteLine($"start to ingest {uri}");
                await m_client.IngestFromStorageAsync(uri:$"{uri};AwsCredentials={awsCredentials}", ingestionProperties: kustoIngestionProperties, sourceOptions);
                Console.WriteLine($"complete to ingest {uri}");
            }
        }
        else {

            Console.WriteLine("no records in event");
        }
    }
}
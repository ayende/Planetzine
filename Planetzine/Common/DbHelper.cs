using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Threading.Tasks;
using System.Configuration;
using HibernatingRhinos.Profiler.Appender.Cosmos;
using Microsoft.Azure.Cosmos;
using System.Runtime.InteropServices;

namespace Planetzine.Common
{
    public class DbHelper
    {
        public static readonly string DatabaseId;
        public static readonly int InitialThroughput;
        public static readonly int MaxConnectionLimit;
        public static readonly ConsistencyLevel ConsistencyLevel;
        public static readonly string EndpointUrl;
        public static readonly string AuthKey;

        public static string CurrentRegion;
        public static CosmosClient Client;
        public static double RequestCharge;

        static DbHelper()
        {
            // Init basic settings
            DatabaseId = ConfigurationManager.AppSettings["DatabaseId"];
            InitialThroughput = int.Parse(ConfigurationManager.AppSettings["InitialThroughput"]);
            MaxConnectionLimit = int.Parse(ConfigurationManager.AppSettings["MaxConnectionLimit"]);
            ConsistencyLevel = (ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), ConfigurationManager.AppSettings["ConsistencyLevel"]);

            EndpointUrl = ConfigurationManager.AppSettings["EndpointURL"];
            AuthKey = ConfigurationManager.AppSettings["AuthKey"];
        }

        /// <summary>
        /// Init() method must be called before using any other methods on DbHelper. Creates the DocumentClient.
        /// </summary>
        /// <returns></returns>
        public static async Task InitAsync()
        {
            CurrentRegion = GetCurrentAzureRegion();

            Client = new CosmosClient(EndpointUrl, AuthKey, new CosmosClientOptions
            {
                ConsistencyLevel = ConsistencyLevel,
                ConnectionMode = (ConnectionMode) Enum.Parse(typeof(ConnectionMode),
                    ConfigurationManager.AppSettings["ConnectionMode"]),
                EnableTcpConnectionEndpointRediscovery = true,
                //GatewayModeMaxConnectionLimit = MaxConnectionLimit,
                MaxRetryAttemptsOnRateLimitedRequests = 10,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromSeconds(10),
                ApplicationPreferredRegions = new List<string> {await GetNearestAzureReadRegionAsync()}
            });
            CosmosDBProfiler.Initialize(Client);
        }

        private static async Task<IEnumerable<AccountRegion>> GetAvailableAzureReadRegionsAsync()
        {
            using (var client = new CosmosClient(EndpointUrl, AuthKey))
            {
                var account = await client.ReadAccountAsync();
                return account.ReadableRegions;
            }
        }

        private static string GetCurrentAzureRegion()
        {
            return Environment.GetEnvironmentVariable("REGION_NAME") ?? "local";
        }

        private static async Task<string> GetNearestAzureReadRegionAsync()
        {
            var regions = (await GetAvailableAzureReadRegionsAsync()).ToDictionary(region => region.Name);
            var currentRegion = GetCurrentAzureRegion();

            // If there is a readable location in the current region, chose it
            if (regions.ContainsKey(currentRegion))
                return currentRegion;

            // Otherwise just pick the first region
            // TODO: Replace this with some logic that selects a more optimal read region (for instance using a table)
            return regions.Values.First().Name;
        }

        public static async Task CreateDatabaseAsync()
        {
            await Client.CreateDatabaseIfNotExistsAsync(DatabaseId);
        }

        public static async Task CreateCollectionAsync(string collectionId, string partitionKey)
        {
            var database = Client.GetDatabase(DatabaseId);
            await database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(collectionId, partitionKey),
                InitialThroughput);
        }

        public static async Task DeleteDatabaseAsync()
        {
            await Client.GetDatabase(DatabaseId).DeleteAsync();
        }

        public static async Task CreateDocumentAsync(object document, string collectionId)
        {
            var response = await Client.GetDatabase(DatabaseId).GetContainer(collectionId)
                .CreateItemAsync(document);
            RequestCharge += response.RequestCharge;
        }

        public static async Task UpsertDocumentAsync(object document, string partitionKey, string collectionId)
        {
            var response = await Client.GetDatabase(DatabaseId).GetContainer(collectionId)
                .UpsertItemAsync(document, new PartitionKey(partitionKey));
            RequestCharge += response.RequestCharge;
        }

        public static async Task<ItemResponse<T>> GetDocumentAsync<T>(string documentId, string partitionKey, string collectionId)
        {
            var response = await Client.GetDatabase(DatabaseId).GetContainer(collectionId)
                .ReadItemAsync<T>(documentId, new PartitionKey(partitionKey));

            RequestCharge += response.RequestCharge;
            
            return response;
        }

        public static async Task DeleteDocumentAsync<T>(string documentId, string partitionKey, string collectionId)
        {
            var response = await Client.GetDatabase(DatabaseId).GetContainer(collectionId)
                .DeleteItemAsync<T>(documentId, new PartitionKey(partitionKey));
            RequestCharge += response.RequestCharge;
        }

        public static async Task DeleteAllDocumentsAsync<T>(string collectionId)
        {
            string sql = $"SELECT c.id, c.partitionId FROM {collectionId} AS c";
            var documents = await ExecuteQueryAsync<dynamic>(collectionId, new QueryDefinition(sql));
            foreach (var document in documents)
            {
                await DeleteDocumentAsync<T>(document.id, document.partitionId, collectionId);
            }
        }

        public static async Task<T[]> ExecuteQueryAsync<T>(string collectionId, QueryDefinition query, string partitionKey = null)
        {
            var requestOptions = new QueryRequestOptions();
            if (partitionKey != null)
                requestOptions.PartitionKey = new PartitionKey(partitionKey);

            var q = Client.GetDatabase(DatabaseId).GetContainer(collectionId)
                .GetItemQueryIterator<T>(query, requestOptions: requestOptions);
            
            var results = new List<T>();
            while (q.HasMoreResults)
            {
                var items = await q.ReadNextAsync();
                results.AddRange(items);
                RequestCharge += items.RequestCharge;
            }

            return results.ToArray();
        }

        public static async Task<T> ExecuteScalarQueryAsync<T>(string sql, string collectionId, string partitionKey)
        {
            var queryRequestOptions = new QueryRequestOptions();
            if (partitionKey != null)
                queryRequestOptions.PartitionKey = new PartitionKey(partitionKey);

            var query = Client.GetDatabase(DatabaseId).GetContainer(collectionId)
                .GetItemQueryIterator<T>(new QueryDefinition(sql), requestOptions: queryRequestOptions);

            var results = new List<T>();
            while (query.HasMoreResults)
            {
                var items = await query.ReadNextAsync();
                results.AddRange(items);
                RequestCharge += items.RequestCharge;
            }

            return results[0];
        }

        public static void ResetRequestCharge()
        {
            RequestCharge = 0.0d;
        }

        public static string Diagnostics()
        {
            var results = $"Server name: {Environment.GetEnvironmentVariable("APPSETTING_WEBSITE_SITE_NAME") ?? "local"} <br/>";
            results += $"Region: {CurrentRegion} <br/>";
            results += $"Total RequestCharge: {RequestCharge:f2} <br/>";
            results += $"EndpointUrl: {EndpointUrl} <br/>";

            results += $"ServiceEndpoint: {Client.Endpoint} <br/>";
            results += $"ConsistencyLevel: {Client.ClientOptions.ConsistencyLevel} <br/>";
            results += $"ConnectionMode: {Client.ClientOptions.ConnectionMode} <br/>";
            results += $"MaxConnectionLimit: {Client.ClientOptions.GatewayModeMaxConnectionLimit} <br/>";
            results += $"PreferredLocations: {string.Join(", ", Client.ClientOptions.ApplicationPreferredRegions)} <br/>";

            return results;
        }
    }
}
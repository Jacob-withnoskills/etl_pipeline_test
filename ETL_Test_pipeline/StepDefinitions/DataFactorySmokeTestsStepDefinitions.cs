using Azure.Core;
using Azure.Identity;
using Azure.ResourceManager.DataFactory;
using Azure.ResourceManager;
using Newtonsoft.Json.Linq;
using TechTalk.SpecFlow;
using Azure.ResourceManager.DataFactory.Models;
using NUnit.Framework;
using Azure;

namespace DataFactorySmokeTests
{
    [Binding, Scope( Tag = "smoke")]
    public class DataFactorySmokeTestsStepDefinitions
    {

        private ArmClient armClient;
        private DataFactoryResource _dataFactory;
        private JObject _config;
        private string _pipelineName = "";

        [BeforeScenario]
        public async Task Setup()
        {
            _config = JObject.Parse(File.ReadAllText(""));
            /* TokenCredential cred = new DefaultAzureCredential();
             * _client = new ArmClient(cred);
             * string subscriptionId = _config["subscriptionId"].ToString();
             * string factoryName = _config["dataFactoryName"].ToString();
             * String clientId = _config["clientId"].ToString();
             * String tenantId = _config["clientId"].ToString();
             * String clientSecret = _config["clientId"].ToString();
             * TokenCredential cl = new ClientSecretCredential(tenantId, clientId, clientSecret);
             * ResourceIdentifier dataFactoryResourceId = DataFactoryResource.CreateResourceIdentifier(subscriptionId, resourceGroupName, factoryName);
             * _dataFactory = _client.GetDataFactoryResource(dataFactoryResourceId); */
            string resourceGroupName = _config["resourceGroupName"].ToString();
            string subscriptionId = _config["subscriptionId"].ToString();
            // 1. Authentication (Interactive)
            //TokenCredential credential = new DefaultAzureCredential();
            TokenCredential credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions
            { ManagedIdentityClientId = "f3cf896b-df2e-429b-84d6-cb81293cd230" }
            );
            // 2. Resource Management Client
            armClient = new ArmClient(credential);
         
            // Get Data Factory Resource
            string dataFactoryName = "lz-int-test-uks-adf-01";
            _dataFactory = await armClient.GetDataFactoryResource(new ResourceIdentifier($"/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.DataFactory/factories/{dataFactoryName}")).GetAsync();
            //  Fetch and Filter Pipelines
            DataFactoryPipelineCollection pipelineCollection = _dataFactory.GetDataFactoryPipelines();
            AsyncPageable<DataFactoryPipelineResource> pipelines = pipelineCollection.GetAllAsync();
            await foreach (DataFactoryPipelineResource pipeline in pipelines)
            {
                Console.WriteLine($"Pipeline Name: {pipeline.Data.Name}");
            }
        }
        [Given(@"an Azure Data Factory scheduled pipeline named ""([^""]*)""")]
        public void GivenAnAzureDataFactoryScheduledPipelineNamed(string pipelineName)
        {
            _pipelineName = pipelineName;
            Console.WriteLine("Name: " + _pipelineName);
        }
        [When(@"the scheduled pipeline is triggered")]
        public async Task WhenTheScheduledPipelineIsTriggeredAtMidnight()
        {
           
            DateTimeOffset endTime = DateTimeOffset.UtcNow;
            DateTimeOffset startTime = endTime.AddDays(-1);
        

            DataFactoryPipelineRunInfo latestRun = null;
         
          
            int maxRetries = 5; // Number of retries to find the pipeline run
            int retryIntervalSeconds = 10; // Wait time between retries

            for (int i = 0; i < maxRetries; i++)
            {
                RunFilterContent filter = new RunFilterContent(startTime, endTime)
                {
                    Filters = { new RunQueryFilter(RunQueryFilterOperand.PipelineName, RunQueryFilterOperator.EqualsValue, new[] { _pipelineName }) },
                    OrderBy = { new RunQueryOrderBy(RunQueryOrderByField.RunStart, RunQueryOrder.Desc) }
                };

                await foreach (DataFactoryPipelineRunInfo run in _dataFactory.GetPipelineRunsAsync(filter))
                {
                    latestRun = run;
                    break; // Exit the loop once a run is found
                }

                if (latestRun != null)
                    break; // Pipeline run found, exit the retry loop

                await Task.Delay(retryIntervalSeconds * 1000); // Wait before the next retry
            }

            // Found a run within 24 hrs return;
            Assert.IsNotNull(latestRun, $"No pipeline runs found for '{_pipelineName}' in the last 24 hours after {maxRetries} retries.");
        



            [Then(@"the pipeline status should be ""([^""]*)"" within (.*) hours")]
            async Task ThenThePipelineStatusShouldBeWithinHours(string expectedStatus, int maxHours)
            {

               
                DateTimeOffset endTime = DateTimeOffset.UtcNow.AddHours(maxHours);
                DateTimeOffset startTime = endTime.AddDays(-1);
                DataFactoryPipelineRunInfo latestRun = null;

                while (DateTimeOffset.UtcNow < endTime)
                {
                    // Fetch the most recent pipeline run
                    RunFilterContent filter = new RunFilterContent(startTime, endTime) 
                    {
                        Filters = {
                new RunQueryFilter(RunQueryFilterOperand.PipelineName, RunQueryFilterOperator.EqualsValue, new string[] { _pipelineName }),
                new RunQueryFilter(RunQueryFilterOperand.Status, RunQueryFilterOperator.NotEquals, new string[] { "InProgress" }) // Exclude "In Progress" runs
            },
                        OrderBy = { new RunQueryOrderBy(RunQueryOrderByField.RunStart, RunQueryOrder.Desc) }
                    };

                    await foreach (DataFactoryPipelineRunInfo run in _dataFactory.GetPipelineRunsAsync(filter))
                    {
                        latestRun = run; // Get the most recent (top) run
                        break;
                    }

                    if (latestRun != null)
                    {
                        // Found a completed run - check status
                        Assert.AreEqual(expectedStatus, latestRun.Status, $"Pipeline '{_pipelineName}' status was '{latestRun.Status}' instead of '{expectedStatus}'.");
                        return; // Exit the loop and test if the status is correct
                    }

                    await Task.Delay(1000); // Check every second
                }

                    Assert.Fail($"Pipeline '{_pipelineName}' did not reach status '{expectedStatus}' within {maxHours} hours or no completed runs were found.");
            }
        }
    }

}

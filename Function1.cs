using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Text;
using System.Linq.Expressions;

namespace RTData
{
    public class GenData
    {
        private readonly ILogger<GenData> _logger;
        private static readonly string EHNamespace = "markm-eh";
        private static readonly string EHName = "transactions";
        private static readonly string EHKeyname = "RootManageSharedAccessKey";
        private static readonly string EHKey = "tMG2u4sBAz+0q87JNMfCRz94+beosl5E3+AEhApoAoY=";
        private static readonly string connectionString = $"Endpoint=sb://markm-eh.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=tMG2u4sBAz+0q87JNMfCRz94+beosl5E3+AEhApoAoY=";
        private static readonly string[] Departments =
        {
            "Mortgage", "Utilities", "Internet", "Phone", "Groceries", "Eating Out", "Transportation", "Healthcare",
            "Insurance", "Personal Care", "Clothing", "Entertainment", "Fitness", "Education", "Gifts", "Travel",
            "Home Maintenance", "Furnishings", "Savings", "Misc"
        };
        private EventHubProducerClient _producerClient;

        public GenData(ILogger<GenData> logger)
        {
            _logger = logger;
            _producerClient = new EventHubProducerClient(connectionString, EHName);
        }

        private static string GetDepartment()
        {
            Random random = new Random();
            int deptNum = random.Next(Departments.Length);
            return Departments[deptNum];
        }

        [Function("GenData")]
        public async Task<IActionResult> RunAsync([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            string action = req.Query["Action"];

            if (string.IsNullOrEmpty(action) || !new[] { "start", "stop", "status" }.Contains(action.ToLower()))
            {
                return new BadRequestObjectResult("Please pass a valid Action parameter (start, stop, status)\nExample: Url?Action=Start");
            }

            string result;

            switch (action.ToLower())
            {
                case "start":
                    try
                    {
                        double balance = 10000;  // Initial balance of $10,000.00
                        Random random = new Random();
                        DateTime dt = DateTime.Now;
                        double price = Math.Round(random.NextDouble() * 1000, 2);
                        string priceStr = price.ToString("F2");

                        // Get the delta time from the query parameters
                        if (!int.TryParse(req.Query["deltaTime"], out int deltaTime))
                        {
                            return new BadRequestObjectResult("Please pass a valid deltaTime parameter in minutes.");
                        }

                        DateTime endTime = DateTime.Now.AddMinutes(deltaTime);
                        result = "Data Generation started will run for " + deltaTime + " minutes";
                        return new OkObjectResult(result);

                        while (DateTime.Now < endTime)
                        {
                            using (EventDataBatch eventBatch = await _producerClient.CreateBatchAsync())
                            {
                                string category = GetDepartment();

                                JObject ehrec = new JObject();

                                if (balance < (price + 500))
                                {
                                    double deposit = Math.Round(random.NextDouble() * 10000, 2);
                                    balance += deposit;
                                    ehrec.Add("DateTime", dt);
                                    ehrec.Add("Category", "Deposit");
                                    ehrec.Add("Deposit", deposit);
                                    ehrec.Add("Withdrawal", "");
                                    ehrec.Add("Balance", balance.ToString("F2"));
                                }
                                else
                                {
                                    balance -= price;
                                    ehrec.Add("DateTime", dt);
                                    ehrec.Add("Category", category);
                                    ehrec.Add("Deposit", "");
                                    ehrec.Add("Withdrawal", priceStr);
                                    ehrec.Add("Balance", balance.ToString("F2"));
                                }

                                eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(ehrec.ToString())));
                                await _producerClient.SendAsync(eventBatch);
                            }

                            // Wait for a short interval before generating the next batch
                            await Task.Delay(1000); // Adjust the delay as needed
                        }

                        result = "Data generation complete.";
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "An error occurred while starting data generation.");
                        return new ObjectResult($"An error occurred: {ex.Message}") { StatusCode = 500 };
                    }
                    break;
                case "stop":
                    result = "Data generation stopped.";
                    break;

                case "status":
                    result = "Data generation status: running.";
                    break;

                default:
                    result = "Invalid action.";
                    break;
            }
            return new OkObjectResult(result);
        }
    }
}
//  08/12/2024 - Mark Moore
// This is a sample program that will randomly generate transactions as you would see in a checkbook along with categories for each transaction and a running balance.
// If the balance falls below the next transaction amount + $500, it will make a random deposit amount to bring the balance back up.
// In this specific example, I am writing the transactions to an Azure Event Hub, however, it would be very easy to write these transactions to a database.
//
// -------------------------------------------------------------------------------------------
//  01/26/2025 - Mark Moore
//  Converted the original code to run as an http triggered Azure funciton with the following paramters passed as an API call.
// 
//  There is an action parameter with the following possible values:
//
//  Start = Start generating events and write them to your event hub
//  -- RunTime = also passed as part of the start command - how many minutes you want the function to generate events if not included will default to 5 min.
//  -- TZ = also passed as part of the start command - your time zone offset
//
//  Status = provide current number of rows written to the event hub and the time the function will stop
//
//  Stop = stop the execution of the function prior to the end time.
// -------------------------------------------------------------------------------------------


using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Text;

// I had to create this static class to hold variables I want to maintain their values for multiple api calls during a single run.
// For example making a subsquent call after the start call to get status or to stop the run.
//
public static class GlobalState
{
    public static int I { get; set; } = 0;
    public static DateTime EndTime { get; set; } = DateTime.MinValue;
    public static string Status { get; set; } = "stopped";
}

public class GenData
{
    private readonly ILogger<GenData> _logger;
    private static readonly string EHName = "transactions";
    private static readonly string connectionString = $"Endpoint=sb://markm-eh.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=tMG2u4sBAz+0q87JNMfCRz94+beosl5E3+AEhApoAoY=";
    private static readonly string[] Departments =
    {
        "Mortgage", "Utilities", "Internet", "Phone", "Groceries", "Eating Out", "Transportation", "Healthcare",
        "Insurance", "Personal Care", "Clothing", "Entertainment", "Fitness", "Education", "Gifts", "Travel",
        "Home Maintenance", "Furnishings", "Savings", "Misc"
    };
    private EventHubProducerClient _producerClient;
    private static CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

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
    public async Task<IActionResult> RunAsync([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req)
    //public async Task<IActionResult> RunAsync([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
    {
        _logger.LogInformation("C# HTTP trigger function processed a request.");

        string action = req.Query["Action"];

        if (string.IsNullOrEmpty(action) || !new[] { "start", "stop", "status" }.Contains(action.ToLower()))
        {
            return new BadRequestObjectResult("Please pass a valid Action parameter (start, stop, status)\nExample: http://<Url>?Action=Start");
        }

        string result;

        switch (action.ToLower())
        {
            case "start":
                try
                {
                    GlobalState.Status = "running";
                    _cancellationTokenSource = new CancellationTokenSource();
                    Console.WriteLine("parameter passed = " + action);
                    double balance = 10000;  // Initial balance of $10,000.00
                    Random random = new Random();
                    DateTime dt = DateTime.Now;
                    double price = Math.Round(random.NextDouble() * 1000, 2);
                    string priceStr = price.ToString("F2");

                    // Get the delta time from the query parameters
                    if (!int.TryParse(req.Query["runTime"], out int deltaTime))
                    {
                        deltaTime = 5;
                        //return new BadRequestObjectResult("Please pass a valid runTime parameter in minutes.");
                    }

                    if (!int.TryParse(req.Query["TZ"], out int timezoneOffset))
                    {
                        timezoneOffset = 0;
                        //return new BadRequestObjectResult("Please pass a valid TZ parameter as an integer representing the timezone offset.");
                    }

                    GlobalState.EndTime = DateTime.Now.AddMinutes(deltaTime);
                    Console.WriteLine("Start time = " + DateTime.Now);
                    Console.WriteLine("Function will run until " + GlobalState.EndTime.ToString());
                    result = "Data Generation started will run for " + deltaTime + " minutes";

                    await Task.Run(async () =>
                    {
                        while (DateTime.Now < GlobalState.EndTime && !_cancellationTokenSource.Token.IsCancellationRequested)
                        {
                            //Console.WriteLine("Inside the while loop");
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
                                //Console.WriteLine(GlobalState.I);
                            }
                            GlobalState.I++;
                            await Task.Delay(1000); // Adjust the delay as needed
                        }
                    }, _cancellationTokenSource.Token);
                    GlobalState.Status = "stopped";
                    result = "Data generation complete. \n" + GlobalState.I + " rows written";
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An error occurred while starting data generation.");
                    result = ($"An error occurred: {ex.Message}");
                    GlobalState.Status = "stopped";
                    break;
                }

            case "stop":
                _cancellationTokenSource.Cancel();
                GlobalState.Status = "stopped";
                result = "Data Generation: " + GlobalState.Status + "\nRows written: " + GlobalState.I;
                break;

            case "status":
                result = "Data generation status: " + GlobalState.Status + "\n" + GlobalState.I + " rows written\nWill run until " + GlobalState.EndTime;
                break;

            default:
                result = "Invalid action.";
                break;
        }
        return new OkObjectResult(result);
    }
}

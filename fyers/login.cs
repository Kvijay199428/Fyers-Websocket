using FyersCSharpSDK;
using HyperSyncLib;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

public partial class Program
{
    public static async Task Main(string[] args)
    {
        // Load environment variables
        string clientID = Environment.GetEnvironmentVariable("CLIENT_ID");
        string secretKey = Environment.GetEnvironmentVariable("SECRET_KEY");
        string redirectURI = Environment.GetEnvironmentVariable("REDIRECT_URI");
        
        // Read access token from file
        string accessToken = File.ReadAllText("api/token/access_token");

        var fyersModel = FyersClass.Instance;
        fyersModel.ClientId = clientID;
        fyersModel.AccessToken = accessToken;

        var dataFetcher = new HistoricalDataFetcher();
        await dataFetcher.FetchTenYearsData(fyersModel);
    }
}

public class HistoricalDataFetcher
{
    private const int MAX_DAYS_PER_REQUEST = 100;
    private const string OUTPUT_DIRECTORY = "historical_data";
    
    public async Task FetchTenYearsData(FyersClass fyersModel)
    {
        // Create output directory if it doesn't exist
        Directory.CreateDirectory(OUTPUT_DIRECTORY);

        // Calculate date ranges
        DateTime endDate = DateTime.Now.Date.AddDays(-1); // Yesterday to avoid partial data
        DateTime startDate = endDate.AddYears(-10);

        // Split into 100-day chunks
        var dateRanges = CalculateDateRanges(startDate, endDate);
        
        foreach (var dateRange in dateRanges)
        {
            try
            {
                await FetchDataForRange(fyersModel, dateRange.startDate, dateRange.endDate);
                // Add delay to avoid rate limiting
                await Task.Delay(1000);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching data for range {dateRange.startDate:yyyy-MM-dd} to {dateRange.endDate:yyyy-MM-dd}");
                Console.WriteLine($"Error: {ex.Message}");
                // Log error and continue with next chunk
                File.AppendAllText(
                    Path.Combine(OUTPUT_DIRECTORY, "error_log.txt"), 
                    $"{DateTime.Now}: Error in range {dateRange.startDate:yyyy-MM-dd} to {dateRange.endDate:yyyy-MM-dd}: {ex.Message}\n"
                );
            }
        }
    }

    private List<(DateTime startDate, DateTime endDate)> CalculateDateRanges(DateTime startDate, DateTime endDate)
    {
        var ranges = new List<(DateTime startDate, DateTime endDate)>();
        var currentStart = startDate;

        while (currentStart < endDate)
        {
            var currentEnd = currentStart.AddDays(MAX_DAYS_PER_REQUEST - 1);
            if (currentEnd > endDate)
                currentEnd = endDate;

            ranges.Add((currentStart, currentEnd));
            currentStart = currentEnd.AddDays(1);
        }

        return ranges;
    }

    private async Task FetchDataForRange(FyersClass fyersModel, DateTime startDate, DateTime endDate)
    {
        var model = new StockHistoryModel
        {
            Symbol = "NSE:SBIN-EQ",
            DateFormat = "1",
            ContFlag = 1,
            Resolution = "1", // 1-minute candles
            RangeFrom = startDate.ToString("yyyy-MM-dd"),
            RangeTo = endDate.ToString("yyyy-MM-dd")
        };

        Console.WriteLine($"Fetching data from {model.RangeFrom} to {model.RangeTo}");

        var stockTuple = await fyersModel.GetStockHistory(model);

        if (stockTuple.Item2 == null)
        {
            // Save data to file
            string fileName = $"SBIN_1min_{startDate:yyyyMMdd}_to_{endDate:yyyyMMdd}.json";
            string filePath = Path.Combine(OUTPUT_DIRECTORY, fileName);
            
            await Task.Run(() => File.WriteAllText(
                filePath,
                JsonConvert.SerializeObject(stockTuple.Item1, Formatting.Indented)
            ));

            Console.WriteLine($"Successfully saved data to {fileName}");
        }
        else
        {
            throw new Exception($"API Error: {stockTuple.Item2}");
        }
    }
}
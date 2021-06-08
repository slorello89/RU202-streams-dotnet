using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace StreamsHandsOnConsumerAggregator
{
    class Program
    {

        static ConnectionMultiplexer _redis = ConnectionMultiplexer.Connect("localhost");
        static async Task Main(string[] args)
        {
            var tasks = new List<Task>();
            tasks.Add(RunAveragingConsumer());
            tasks.Add(RunConsumer());
            tasks.Add(RunProducer());
            Task.WaitAll(tasks.ToArray());
        }

        static async Task RunProducer()
        {
            
            var db = _redis.GetDatabase();
            while (true)
            {
                var transaction = db.CreateTransaction();

                var stream = $"temps:{DateTime.Now.ToString("yyyyMMdd")}";

                var result = transaction.StreamAddAsync(stream, "temp", "55");
                _ = transaction.KeyExpireAsync(stream, DateTime.Now.AddDays(2));
                await transaction.ExecuteAsync();
                Console.WriteLine(result.Result);
                await Task.Delay(1000);
            }
        }

        static async Task RunConsumer()
        {
            var id = "0";
            var streamName = $"temps:{DateTime.Now.ToString("yyyyMMdd")}:averages";            
            var db = _redis.GetDatabase();            
            while (true)
            {
                var next = await db.StreamReadAsync(streamName, id, 1);
                if (next.Length > 0)
                {
                    var avg = next[0];
                    Console.WriteLine($"Average temperature for hour:{avg["hour"]} is {avg["average_temp_f"]} based on {avg["num_observations"]} observations");
                    id = avg.Id;
                }
                else
                {
                    await Task.Delay(1000);
                }
            }
        }

        static async Task RunAveragingConsumer()
        {
            // build connection            
            var database = _redis.GetDatabase();

            // Generate read stream name from day
            var streamNameTimeStamp = DateTime.Now;
            var streamName = $"temps:{streamNameTimeStamp.ToString("yyyyMMdd")}";

            // Read the first message from the stream
            var streamStart = await database.StreamReadAsync(streamName, "0", 1);

            //build inital timestamp
            var id = streamStart[0].Id.ToString();
            var timeInt = long.Parse(id.Split('-')[0]);
            var timestamp = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            timestamp = timestamp.AddMilliseconds(timeInt);

            // set up inital values
            var currentHour = timestamp.Hour;
            var totalTempCounter = int.Parse(streamStart[0]["temp"]);
            var numMeasurementsThisHour = 1;

            while (true)
            {
                // if the current date differs from the stream's that
                // we are looking at we need to reset some stuff
                if (streamNameTimeStamp.Date != DateTime.Now.Date)
                {
                    streamNameTimeStamp = DateTime.Now;
                    streamName = $"temps:{streamNameTimeStamp.ToString("yyyyMMdd")}";
                    id = "0";
                }

                // read id out of database
                var response = await database.StreamReadAsync(streamName, id, 1);                
                if (response.Length > 0)
                {
                    id = response[0].Id;
                    timeInt = long.Parse(id.Split('-')[0]);
                    timestamp = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    timestamp = timestamp.AddMilliseconds(timeInt);
                    if (timestamp.Hour != currentHour)
                    {
                        var averagesStreamNameKey = $"{streamName}:averages";
                        var average = totalTempCounter / numMeasurementsThisHour;
                        var payload = new NameValueEntry[] {
                            new NameValueEntry("hour",currentHour),
                            new NameValueEntry("date",streamNameTimeStamp.ToString("yyyyMMdd")),
                            new NameValueEntry("average_temp_f", average),
                            new NameValueEntry("num_observations", numMeasurementsThisHour)
                        };
                        Console.WriteLine($"Writing to {averagesStreamNameKey}");

                        database.StreamAdd(averagesStreamNameKey, payload);
                        totalTempCounter = 0;
                        numMeasurementsThisHour = 0;

                        currentHour = timestamp.Hour;
                    }

                    totalTempCounter += int.Parse(response[0]["temp"]);
                    numMeasurementsThisHour += 1;
                }
                else
                {
                    // if we don't have any messages to process, sleep for a second
                    await Task.Delay(1000);
                }
            }
        }
    }
}

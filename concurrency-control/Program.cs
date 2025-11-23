using concurrency_control;
using Microsoft.Data.Sqlite;
using System.Security.Cryptography;

class Program
{
    const string DSN = "Data Source=concurrency.db;";

    static void Main()
    {
        Setup();
        RunSimulation().Wait();
    }

    static void Setup()
    {
        if (!File.Exists("concurrency.db"))
        {
            using (var connection = new SqliteConnection(DSN))
            {
                connection.Open();

                var createTableCommand = connection.CreateCommand();
                createTableCommand.CommandText =
                """
                    CREATE TABLE IF NOT EXISTS RequestQueue (
                        Id INTEGER PRIMARY KEY,
                        ActivationTime INTEGER,
                        TimeoutTime INTEGER NOT NULL
                    );
                """;
                createTableCommand.ExecuteNonQuery();
            }
        }
    }

    static async Task RunSimulation()
    {
        using (var connection = new SqliteConnection(DSN))
        {
            connection.Open();
            using (var walModeCmd = connection.CreateCommand())
            {
                walModeCmd.CommandText = "PRAGMA journal_mode = WAL;";
                walModeCmd.ExecuteNonQuery();
            }

            Console.WriteLine("Simulating requests (1 at a time per execution)");
            Console.WriteLine();
            while (true)
            {
                var rq = new RequestQueue(connection, maxConcurrentRequests: 5);
                await SimulateRequest(rq);
                await Task.Delay(RandomNumberGenerator.GetInt32(1500));
                Console.WriteLine();
            }
        }
    }

    static async Task SimulateRequest(RequestQueue rq)
    {
        try
        {
            PriorityLevel priority = RandomNumberGenerator.GetInt32(3) switch
            {
                1 => PriorityLevel.HIGH,
                2 => PriorityLevel.MEDIUM,
                _ => PriorityLevel.LOW,
            };

            Console.WriteLine($"Initiating task with priority {Enum.GetName(typeof(PriorityLevel), priority)}");
            var start = DateTime.Now;

            var ttl = 5;
            var slot = await rq.ReserveSlot(priority, ttl);
            Console.WriteLine($"Task reserved with priority {Enum.GetName(typeof(PriorityLevel), priority)} after {(DateTime.Now - start).TotalMilliseconds}ms");
            if (RandomNumberGenerator.GetInt32(500) == 0)
            { // 0.2% chance to fail silently after reservation (ex: job timeout, server restart, etc.)
                Console.WriteLine($"Task failed after reservation (1 in 500 chance)");
                return;
            }

            var processingTime = 10 + RandomNumberGenerator.GetInt32(4500);
            Console.WriteLine($"Faking Processing time of {processingTime}ms");
            await Task.Delay(processingTime);
            Console.WriteLine($"Task done with priority {Enum.GetName(typeof(PriorityLevel), priority)} after {(DateTime.Now - start).TotalMilliseconds}ms");

            await rq.FreeSlot(slot);
            Console.WriteLine($"Task freed");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
}

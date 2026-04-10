using Microsoft.Data.Sqlite;
// SqlServer: Microsoft.Data.SqlClient;
using System.Security.Cryptography;

namespace ConcurrencyControl
{
    /// <summary>
    /// A database-backed distributed concurrency controller (semaphore) that limits simultaneous operations 
    /// across multiple threads or processes.
    /// 
    /// Usage Flow:
    /// 1. Await <see cref="ReserveSlot"/> to claim a concurrency slot. If the queue is at capacity for the requested priority, it will asynchronously poll until a slot opens.
    /// 2. Execute the rate-limited work.
    /// 3. Await <see cref="FreeSlot"/> to release the slot for the next requester.
    /// 
    /// Key Features:
    /// - Priority Allocation: Reserves a percentage of total capacity for higher-priority tasks to prevent starvation from low-priority floods.
    /// - Fault Tolerance: Slots automatically expire after a specified TTL, preventing permanent deadlocks if a worker process crashes mid-execution.
    /// </summary>
    ///
    /// Sqlite required table structure
    ///
    /// CREATE TABLE RequestQueue (
    ///     Id INTEGER PRIMARY KEY,
    ///     ActivationTime INTEGER,
    ///     TimeoutTime INTEGER NOT NULL
    /// );
    ///
    /// SqlServer required table structure
    ///
    /// CREATE TABLE RequestQueue (
    ///     Id BIGINT PRIMARY KEY,
    ///     ActivationTime DATETIME2(0) NULL,
    ///     TimeoutTime DATETIME2(0) NOT NULL
    /// );
    class RequestQueue
    {
        private const int TIMEOUT = 60; // Wait TIMEOUT seconds considering data stale

        protected SqliteConnection connection;
        // SqlServer: protected SqlConnection connection;
        protected int maxConcurrentRequests;
        public RequestQueue(SqliteConnection connection, int maxConcurrentRequests)
        // SqlServer: public RequestQueue(SqlConnection connection, int maxConcurrentRequests)
        {
            this.connection = connection;
            this.maxConcurrentRequests = maxConcurrentRequests;
        }

        public async Task<Int64> ReserveSlot(PriorityLevel priority, int ttl)
        {
            var maxReservedSlot = GetPriorityMaxReservedSlot(priority);

            using var registerCmd = connection.CreateCommand();
            registerCmd.CommandText = "INSERT INTO RequestQueue (Id, TimeoutTime) VALUES (@id, unixepoch('now') + " + TIMEOUT + ");";
            // SqlServer: registerCmd.CommandText = "INSERT INTO RequestQueue (Id, TimeoutTime) VALUES (@id, DATEADD(second," + TIMEOUT + ", GETUTCDATE()));";

            registerCmd.Parameters.AddWithValue("id", 0);
            // SqlServer: registerCmd.Parameters.AddWithValue("@id", 0);


            using var reserveCmd = connection.CreateCommand();
            reserveCmd.CommandText = "UPDATE RequestQueue SET ActivationTime = unixepoch('now'), TimeoutTime = unixepoch('now') + @ttl WHERE id = @id AND (SELECT COUNT(*) < " + maxReservedSlot + " FROM RequestQueue WHERE ActivationTime IS NOT NULL AND TimeoutTime > unixepoch('now'))";
            // SqlServer: reserveCmd.CommandText = "UPDATE RequestQueue SET ActivationTime = GETUTCDATE(), TimeoutTime = DATEADD(second, @ttl, GETUTCDATE()) WHERE id = @id AND (SELECT COUNT(*) FROM RequestQueue WITH (UPDLOCK, HOLDLOCK) WHERE ActivationTime IS NOT NULL AND TimeoutTime > GETUTCDATE()) < " + maxReservedSlot;
            reserveCmd.Parameters.AddWithValue("ttl", ttl);
            // SqlServer: reserveCmd.Parameters.AddWithValue("@ttl", ttl);
            reserveCmd.Parameters.AddWithValue("id", 0);
            // SqlServer: reserveCmd.Parameters.AddWithValue("@id", 0);

            Int64 id = 0;
            DateTime creationTime = DateTime.MinValue;
            bool reserved = false;
            do
            {
                if (creationTime.AddSeconds(TIMEOUT) < DateTime.Now || id == 0)
                {
                    id = RandomNumberGenerator.GetInt32(Int32.MaxValue);
                    creationTime = DateTime.Now;
                    registerCmd.Parameters["id"].Value = id;
                    // SqlServer: registerCmd.Parameters["@id"].Value = id;
                    await registerCmd.ExecuteNonQueryAsync();

                    reserveCmd.Parameters["id"].Value = id;
                    // SqlServer: reserveCmd.Parameters["@id"].Value = id;
                }

                try
                {
                    using (var transaction = connection.BeginTransaction(System.Data.IsolationLevel.Serializable))
                    {
                            reserveCmd.Transaction = transaction;
                            reserved = await reserveCmd.ExecuteNonQueryAsync() >= 1;
                            await transaction.CommitAsync();
                    }
                }
                catch (Exception) // DB is locked
                // SqlServer: catch(SqlException)
                {
                    reserved = false;
                }
                finally
                {
                    reserveCmd.Transaction = null;
                }

                if (!reserved)
                {
                    await Task.Delay(50);
                }
            }
            while (!reserved);

            return id;
        }

        private int GetPriorityMaxReservedSlot(PriorityLevel priority)
        {
            return priority switch
            {
                PriorityLevel.HIGH => maxConcurrentRequests,
                PriorityLevel.MEDIUM => Math.Max(1, Convert.ToInt32(maxConcurrentRequests * 0.8)),
                _ => Math.Max(1, Convert.ToInt32(maxConcurrentRequests * 0.6)),
            };
        }

        public async Task FreeSlot(Int64 id)
        {
            using var freeCmd = connection.CreateCommand();
            // Cleans up old slots at the same time
            freeCmd.CommandText = "DELETE FROM RequestQueue WHERE id = @id OR TimeoutTime <= unixepoch('now');";
            // SqlServer: freeCmd.CommandText = "DELETE FROM RequestQueue WHERE id = @id OR TimeoutTime <= GETUTCDATE();";
            freeCmd.Parameters.AddWithValue("id", id);
            // SqlServer: freeCmd.Parameters.AddWithValue("@id", id);
            await freeCmd.ExecuteNonQueryAsync();
        }
    }

    enum PriorityLevel
    {
        HIGH = 0,
        MEDIUM = 1,
        LOW = 2,
    }
}

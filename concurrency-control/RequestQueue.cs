using Microsoft.Data.Sqlite;
using System.Security.Cryptography;

namespace concurrency_control
{
    class RequestQueue
    {
        private const int TIMEOUT = 60; // Wait TIMEOUT seconds considering data stale

        protected SqliteConnection connection;
        protected int maxConcurrentRequests;
        public RequestQueue(SqliteConnection connection, int maxConcurrentRequests)
        {
            this.connection = connection;
            this.maxConcurrentRequests = maxConcurrentRequests;
        }

        public async Task<Int64> ReserveSlot(PriorityLevel priority, int ttl)
        {
            var maxReservedSlot = GetPriorityMaxReservedSlot(priority);

            using var registerCmd = connection.CreateCommand();
            registerCmd.CommandText = "INSERT INTO RequestQueue (Id, TimeoutTime) VALUES (@id, unixepoch('now') + " + TIMEOUT + ");";
            registerCmd.Parameters.AddWithValue("id", 0);

            using var reserveCmd = connection.CreateCommand();
            reserveCmd.CommandText = "UPDATE RequestQueue SET ActivationTime = unixepoch('now'), TimeoutTime = unixepoch('now') + @ttl WHERE id = @id AND (SELECT COUNT(*) < " + maxReservedSlot + " FROM RequestQueue WHERE ActivationTime IS NOT NULL AND TimeoutTime > unixepoch('now'))";
            reserveCmd.Parameters.AddWithValue("ttl", ttl);
            reserveCmd.Parameters.AddWithValue("id", 0);
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
                    registerCmd.ExecuteNonQuery();

                    reserveCmd.Parameters["id"].Value = id;
                }

                reserved = reserveCmd.ExecuteNonQuery() >= 1;
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
            freeCmd.CommandText = "DELETE FROM RequestQueue WHERE id = @id OR TimeoutTime <= unixepoch('now');"; // Cleans up old slots at the same time
            freeCmd.Parameters.AddWithValue("id", id);
            freeCmd.ExecuteNonQuery();
        }
    }

    enum PriorityLevel
    {
        HIGH = 0,
        MEDIUM = 1,
        LOW = 2,
    }
}

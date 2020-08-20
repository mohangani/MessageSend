using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace MessageSend
{
    class Program
    {
        // Connection String for the namespace can be obtained from the Azure portal under the 
        // 'Shared Access policies' section.
        const string ServiceBusConnectionString = "Endpoint=sb://sbquetest.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ejxMtsx2c8PDC4fjhUfKHewx6be/UjYwOSVuEDFIhUI=;TransportType=AmqpWebSockets";
        const string QueueName = "sbtest";
        static IQueueClient queueClient;
        public static void Main(string[] args)
        {
            const int numberOfMessages = 1;
            try
            {
                queueClient = new QueueClient(ServiceBusConnectionString,
                           QueueName,
                            ReceiveMode.PeekLock,
                             new RetryExponential(TimeSpan.FromSeconds(.5), TimeSpan.FromSeconds(1), 3));

                Console.WriteLine("Job Started ....");
                //CancellationTokenSource cts = new CancellationTokenSource();

                // Send messages.
                //for (int index = 0; index < numberOfMessages; index++)
                //{
                //    SendMessagesAsync(numberOfMessages).GetAwaiter().GetResult();
                //}


                Parallel.For(0, numberOfMessages, x => SendMessagesAsync(numberOfMessages).GetAwaiter().GetResult());
                //Console.ReadKey();
                Console.WriteLine("Job Done");
                Console.ReadKey();
                queueClient.CloseAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception : {ex.Message}");
                Console.ReadLine();
                throw;
            }

        }

        static async Task SendMessagesAsync(int numberOfMessagesToSend)
        {
            Message MyMessage = null;
            try
            {
                //Thread.Sleep(1);//1 millisecond timeout
                // Create a new message to send to the queue
                // Interlocked.Increment(ref counter);
                MyMessage = new Message()
                {
                    ContentType = "JSON",
                    TimeToLive = TimeSpan.FromDays(30),
                    CorrelationId = Guid.NewGuid().ToString(),
                    Body = System.IO.File.ReadAllBytes(@"E:\ServiceBus Test\ServiceBus Test Files\TestFile.txt"),
                };
               // MyMessage.MessageId = Encoding.UTF8.GetString(MyMessage.Body).GetHashCode().ToString();
                MyMessage.MessageId = ComputeSha256Hash(MyMessage.Body);
                //long totalsize = MyMessage.Size; //+ MyMessage.UserProperties + MyMessage.SystemProperties;
                Console.WriteLine($"Sending message: {MyMessage.MessageId}");
                //Write the Message into Some Physical path For Count

                Directory.CreateDirectory(@"E:\Service Bus Data Testing\Transmitter\");
                File.WriteAllText(@"E:\Service Bus Data Testing\Transmitter\" + MyMessage.CorrelationId + ".txt", Encoding.UTF8.GetString(MyMessage.Body));

                // Send the message to the queue
                await queueClient.SendAsync(MyMessage);
            }
            catch (Exception exception)
            {
                Directory.CreateDirectory(@"E:\Service Bus Data Testing\TransmissionFailed\");
                //Logging the Failed Message 
                System.IO.File.WriteAllText(@"E:\Service Bus Data Testing\TransmissionFailed\" + MyMessage.CorrelationId + ".txt", Encoding.UTF8.GetString(MyMessage.Body));
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static string ComputeSha256Hash(byte[] messagebody)
        {
            // Create a SHA256   
            using (SHA256 sha256Hash = SHA256.Create())
            {
                // ComputeHash - returns byte array  
                var bytes = sha256Hash.ComputeHash(messagebody);

                // Convert byte array to a string   
                StringBuilder builder = new StringBuilder();
                foreach (var item in bytes)
                {
                    builder.Append(item.ToString("x2"));
                }

                //for (int i = 0; i < bytes.Length; i++)
                //{
                //    builder.Append(bytes[i].ToString("x2"));
                //}
                return builder.ToString();
            }
        }

    }
}

using System;
using System.Messaging;
using System.Diagnostics;

namespace Webhive.Blog.MsmqOperations {
    class MsmqActions {
        public static void CreateQueue(string queueName) {
            if (MessageQueue.Exists(queueName)) { Console.WriteLine("Queue '{0}' already exists.", queueName); 
            }

            try {
                var queue = MessageQueue.Create(queueName);
                queue.Dispose();

                Console.WriteLine("Private queue '{0}' successfully created.", queueName);
            }
            catch (MessageQueueException ex) {
                Console.WriteLine("Error creating queue '{0}':\n{1}", queueName, ex);
            }
        }

        public static void EnqueueMessages<T>(string queueName, T message, int numberOfMessages) {
            var stopwatch = Stopwatch.StartNew();
            var queue = new MessageQueue(queueName) { Formatter = new BinaryMessageFormatter() };

            try {
                for (var index = 0; index < numberOfMessages; ++index) {
                    var msg = new Message(message) { Formatter = new BinaryMessageFormatter() };
                    queue.Send(msg);
                }

                stopwatch.Stop();
                Console.WriteLine("Successfully enqueued '{0}' messages in '{1}' milliseconds.", numberOfMessages, stopwatch.ElapsedMilliseconds);
            }
            catch (MessageQueueException ex) {
                Console.WriteLine("Error enqueuing messages: {0}", ex);
            }

            queue.Dispose();
        }

        public static void PurgeQueue(string queueName) {
            var queue = new MessageQueue(queueName) { Formatter = new BinaryMessageFormatter() };

            try {
                queue.Purge();
                Console.WriteLine("Successfully purged '{0}'.", queueName);
            }
            catch (MessageQueueException ex) {
                Console.WriteLine("Error purging {0}:\n{1}", queueName, ex);
            }

            queue.Dispose();
        }

        public static void DequeueMessages(string queueName) {
            //
        }
    }
}
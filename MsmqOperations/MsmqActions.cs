using System;
using System.Messaging;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Webhive.Blog.MsmqOperations {
    interface IMessageFormatterCreator {
        IMessageFormatter CreateNewInstance();
    }

    class BinaryMessageFormaterCreator : IMessageFormatterCreator {
        public IMessageFormatter CreateNewInstance() {
            return new BinaryMessageFormatter();
        }
    }

    class XmlMessageFormatterCreator : IMessageFormatterCreator {
        public IMessageFormatter CreateNewInstance() {
            return new XmlMessageFormatter();
        }
    }

    class ActiveXMessageFormatterCreator : IMessageFormatterCreator {
        public IMessageFormatter CreateNewInstance() {
            return new ActiveXMessageFormatter();
        }
    }


    class MsmqActions {
        readonly IMessageFormatterCreator _messageFormatterCreator;

        private MsmqActions(IMessageFormatterCreator messageFormatterCreator) {
            _messageFormatterCreator = messageFormatterCreator;
        }

        public static MsmqActions WithFormatter(IMessageFormatterCreator messageFormatterCreator) {
            return new MsmqActions(messageFormatterCreator);
        }


        public void CreateQueue(string queueName) {
            if (MessageQueue.Exists(queueName)) {
                Console.WriteLine("Queue '{0}' already exists.", queueName);
                return;
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

        public void EnqueueMessages<T>(string queueName, T message, int numberOfMessages) {
            var stopwatch = Stopwatch.StartNew();
            var queue = new MessageQueue(queueName) { Formatter = _messageFormatterCreator.CreateNewInstance() };

            try {
                for (var index = 0; index < numberOfMessages; ++index) {
                    var msg = new Message(message) { Formatter = _messageFormatterCreator.CreateNewInstance(), Label = index.ToString() };
                    queue.Send(msg);
                }
            }
            catch (MessageQueueException ex) {
                Console.WriteLine("Error enqueuing messages: {0}", ex);
            }
            finally {
                stopwatch.Stop();
            }

            Console.WriteLine("Successfully enqueued '{0}' messages in '{1}' milliseconds.", numberOfMessages, stopwatch.ElapsedMilliseconds);

            queue.Dispose();
        }

        public void PurgeQueue(string queueName) {
            var queue = new MessageQueue(queueName) { Formatter = _messageFormatterCreator.CreateNewInstance() };

            try {
                queue.Purge();
                Console.WriteLine("Successfully purged '{0}'.", queueName);
            }
            catch (MessageQueueException ex) {
                Console.WriteLine("Error purging {0}:\n{1}", queueName, ex);
            }

            queue.Dispose();
        }

        public void DequeueMessagesBulk<T>(string queueName, Action<T> doThis) {
            var stopwatch = Stopwatch.StartNew();
            var queue = new MessageQueue(queueName) { Formatter = _messageFormatterCreator.CreateNewInstance() };

            try {
                var msgs = queue.GetAllMessages();
                var msgCount = 0;

                foreach (var msg in msgs) {
                    msg.Formatter = _messageFormatterCreator.CreateNewInstance();
                    var dequeued = queue.ReceiveById(msg.Id); // Get the message off the queue.

                    if (dequeued != null) {
                        msgCount++;
                        Task.Factory.StartNew(() => { doThis.Invoke((T) dequeued.Body); });
                    }
                }

                stopwatch.Stop();
                Console.WriteLine("Successfully dequeued '{0}' messages in '{1}' milliseconds.", msgCount, stopwatch.ElapsedMilliseconds);
            }
            catch (MessageQueueException ex) {
                Console.WriteLine("Error dequeuing messages: {0}", ex);
            }

            queue.Dispose();
        }

        public void DequeueMessagesSequential<T>(string queueName, Action<T> doThis) {
            var stopwatch = Stopwatch.StartNew();
            var queue = new MessageQueue(queueName) { Formatter = _messageFormatterCreator.CreateNewInstance() };

            try {
                var msgCount = 0;

                while (true) {
                    Message msg = null;

                    try {
                        msg = queue.Receive(TimeSpan.FromSeconds(1));
                    }
                    catch (MessageQueueException timeout) {
                        if (timeout.Message.IndexOf("Timeout for the requested operation has expired.", StringComparison.Ordinal) >= 0) {
                            stopwatch.Stop();
                            break;
                        }
                    }

                    if (msg != null) {
                        msgCount++;
                        msg.Formatter = _messageFormatterCreator.CreateNewInstance();
                        Task.Factory.StartNew(() => { doThis.Invoke((T) msg.Body); });
                    }
                }

                Console.WriteLine("Successfully dequeued '{0}' messages in '{1}' milliseconds.", msgCount, stopwatch.ElapsedMilliseconds - 1000); // Subtract the 1 second receive-wait-time.
            }
            catch (MessageQueueException ex) {
                Console.WriteLine("Error dequeuing messages: {0}", ex);
            }

            queue.Dispose();
        }
    }
}

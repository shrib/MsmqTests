using System;

namespace Webhive.Blog.MsmqOperations {
    class Program {
        static string _queueName = "";
        private static void Main() {
            Console.Write("Enter queue name: ");
            _queueName = Console.ReadLine();

            PromptForAction();

            Console.ReadLine();
        }

        private static void PromptForAction() {
            var optionSelected = "-1";

            Console.WriteLine("1. Enqueue messages");
            Console.WriteLine("2. Dequeue-Bulk messages");
            Console.WriteLine("3. Dequeue-Sequential messages");
            Console.WriteLine("4. Purge messages from a queue");
            Console.WriteLine("0. Exit");
            Console.WriteLine();

            while (!optionSelected.Equals("0")) {
                optionSelected = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(optionSelected))
                    optionSelected = "";
                else
                    DoAction(optionSelected);
            }
        }

        private static void DoAction(string optionSelected) {
            Console.WriteLine();

            switch (optionSelected) {
                case "1":
                    EnqueueMessages();
                    break;

                case "2":
                    DequeueMessages();
                    break;

                case "3":
                    DequeueMessages(false);
                    break;
            }

            Console.WriteLine();
        }

        static void EnqueueMessages() {
            Console.Write("Message count: ");
            var input = Console.ReadLine();
            var count = int.Parse(input ?? "0");
            Console.Write("Formatter (1 = Binary, 2 = Xml, 3 = ActiveX): ");
            input = Console.ReadLine();
            var selectedFormatter = int.Parse(input ?? "1");

            // NOTE: May clog the local desktop if we're just queueing a lot of messages without.
            count = Math.Min(count, 100000);

            var data = Guid.NewGuid().ToString();
            data = string.Format("{0},{0},{0},{0}", data);

            MsmqActions
                .WithFormatter(GetFormatter(selectedFormatter))
                .EnqueueMessages(_queueName, data, count);
        }

        static void DequeueMessages(bool bulk = true) {
            Console.Write("Formatter (1 = Binary, 2 = Xml, 3 = ActiveX): "); // Xml doesn't support Bulk dequeue in this version
            var input = Console.ReadLine();
            var selectedFormatter = int.Parse(input ?? "1");

            var act = new Action<string>(x => { /* Do something here with the dequeued message */ });

            if (bulk) {
                MsmqActions
                    .WithFormatter(GetFormatter(selectedFormatter))
                    .DequeueMessagesBulk(_queueName, act);
            }
            else {
                MsmqActions
                    .WithFormatter(GetFormatter(selectedFormatter))
                    .DequeueMessagesSequential(_queueName, act);
            }
        }

        static IMessageFormatterCreator GetFormatter(int selection) {
            switch (selection) {
                case 2:
                    return new XmlMessageFormatterCreator();

                case 3:
                    return new ActiveXMessageFormatterCreator();

                default:
                    return new BinaryMessageFormaterCreator();
            }
        }
    }
}

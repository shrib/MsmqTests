using System;

namespace Webhive.Blog.MsmqOperations {
    class Program {
        private static void Main() {
            PromptForAction();

            Console.ReadLine();
        }

        private static void PromptForAction() {
            var optionSelected = "-1";

            Console.WriteLine("1. Create a queue");
            Console.WriteLine("2. Purge messages from a queue");
            Console.WriteLine("3. Enqueue messages");
            Console.WriteLine("4. Dequeue messages");
            Console.WriteLine("5. Delete queue");
            Console.WriteLine("0. Exit");

            while (!optionSelected.Equals("0")) {
                optionSelected = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(optionSelected))
                    optionSelected = "";
                else
                    DoAction(optionSelected);
            }
        }

        private static void DoAction(string optionSelected) {
            switch (optionSelected) {
                case "1":
                    Program.CreateQueue();
                    break;
            }
        }

        private static void CreateQueue() {
            Console.WriteLine("Queue Name: ");
            var queueName = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(queueName))
                Console.WriteLine("Invalid queue name specified. Aborting operation.");
            else
                MsmqActions.CreateQueue(queueName);
        }
    }
}

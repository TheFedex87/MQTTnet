using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MQTTClientTest
{
    class Program
    {
        static void Main(string[] args)
        {
            
            while (true)
            {
                System.Threading.Thread.Sleep(10);
                var operation = Console.ReadKey();
                if (operation.Key == ConsoleKey.D1)
                {
                    connectAndRegisterToTopic();
                }
                else if (operation.Key == ConsoleKey.D2)
                {
                    Console.WriteLine("Type the message to send and click enter");
                    string message = Console.ReadLine();
                    connectAndSendToTopic(message);
                }
            }
        }

        static async void connectAndSendToTopic(string messageToSend)
        {
            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();
            var options = new MqttClientOptionsBuilder().WithTcpServer("127.0.0.1").Build();
            try
            {
                var conn = await mqttClient.ConnectAsync(options, CancellationToken.None); // Since 3.0.5 with CancellationToken             

                //mqttClient.UseApplicationMessageReceivedHandler(e =>
                //{
                //    Console.WriteLine("### RECEIVED APPLICATION MESSAGE ###");
                //    Console.WriteLine($"+ Topic = {e.ApplicationMessage.Topic}");
                //    Console.WriteLine($"+ Payload = {Encoding.UTF8.GetString(e.ApplicationMessage.Payload)}");
                //    Console.WriteLine($"+ QoS = {e.ApplicationMessage.QualityOfServiceLevel}");
                //    Console.WriteLine($"+ Retain = {e.ApplicationMessage.Retain}");
                //    Console.WriteLine();

                //    //Task.Run(() => mqttClient.PublishAsync("my/topic"));
                //});

                var message = new MqttApplicationMessageBuilder()
                    .WithTopic("my/topicc")
                    .WithPayload(messageToSend)
                    .WithExactlyOnceQoS()
                    .Build();

                var res = await mqttClient.PublishAsync(message, CancellationToken.None); // Since 3.0.5 with CancellationToken
            }
            catch (Exception ex)
            {

            }

            Console.WriteLine("Connected");
        }

        static async void connectAndRegisterToTopic()
        {
           
            var factory = new MqttFactory();
            var mqttClient = factory.CreateMqttClient();
            var options = new MqttClientOptionsBuilder().WithTcpServer("127.0.0.1").Build();
            try
            {
                mqttClient.UseConnectedHandler(async e =>
                {
                    Console.WriteLine("### CONNECTED WITH SERVER ###");

                    // Subscribe to a topic
                    await mqttClient.SubscribeAsync(new MqttTopicFilterBuilder().WithTopic("my/topicc").Build());

                    Console.WriteLine("### SUBSCRIBED ###");
                });

                var conn = await mqttClient.ConnectAsync(options, CancellationToken.None); // Since 3.0.5 with CancellationToken             

                mqttClient.UseApplicationMessageReceivedHandler(e =>
                {
                    Console.WriteLine("### RECEIVED APPLICATION MESSAGE ###");
                    Console.WriteLine($"+ Topic = {e.ApplicationMessage.Topic}");
                    Console.WriteLine($"+ Payload = {Encoding.UTF8.GetString(e.ApplicationMessage.Payload)}");
                    Console.WriteLine($"+ QoS = {e.ApplicationMessage.QualityOfServiceLevel}");
                    Console.WriteLine($"+ Retain = {e.ApplicationMessage.Retain}");
                    Console.WriteLine();

                    //Task.Run(() => mqttClient.PublishAsync("my/topic"));
                });

                /*var message = new MqttApplicationMessageBuilder()
                    .WithTopic("my/topicc")
                    .WithPayload("Hello World!!!!!!!!!!")
                    .WithExactlyOnceQoS()
                    .Build();

                var res = await mqttClient.PublishAsync(message, CancellationToken.None);*/ // Since 3.0.5 with CancellationToken
            } catch(Exception ex)
            {

            }
            
            Console.WriteLine("Connected");
        }
    }
}

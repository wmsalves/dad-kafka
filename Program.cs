using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "dad-pucwemerson.servicebus.windows.net:9093",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = "Endpoint=sb://dad-pucwemerson.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=TxBtl3L3+Wbi3sqTBmtmMwa5G9XL90CXe+AEhApbGtY="
            };

            var topic = "dad-kafka";

            var message = new
            {
                name = "Wemerson Alves Santana",
                login_id = "1349018@sga.pucminas.br",
                group = 2
            };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                var jsonMessage = Newtonsoft.Json.JsonConvert.SerializeObject(message);
                var deliveryReport = await producer.ProduceAsync(topic, new Message<string, string> { Key = null});

                Console.WriteLine(jsonMessage);
                Console.WriteLine($"Mensagem enviada para: {deliveryReport.TopicPartitionOffset}");
                Console.WriteLine("Wemerson Alves Santana");


            }
        }
    }
}

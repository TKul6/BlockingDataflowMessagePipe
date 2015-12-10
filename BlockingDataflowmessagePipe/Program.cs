using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BlockingDataflowmessagePipe
{
    class Program
    {
        static void Main(string[] args)
        {
            BlockingDataflowMessagePipe<string> producerPipe;

            BlockingDataflowMessagePipe<string> readerPipe;

            BlockingDataflowMessagePipe<byte[]> parserPipe;

            BlockingDataflowMessagePipe<byte[]> writerPipe;


            writerPipe = new BlockingDataflowMessagePipe<byte[]>(message =>
            {
          

                File.WriteAllBytes(@"c:\lev\outputs\" + Guid.NewGuid() + ".zip",message);
            },1,2,2);


            parserPipe = new BlockingDataflowMessagePipe<byte[]>(message =>

            {
                var newMessage = new byte[message.Length];

                message.CopyTo(newMessage,0);

                if (!writerPipe.AddMessage(newMessage))
                {
                    Console.WriteLine("Parser could not send to writer");
                }

            },1,2,2);


            readerPipe = new BlockingDataflowMessagePipe<string>(message =>
            {
                var bytes = File.ReadAllBytes(@"c:\lev\package.zip");

                if (!parserPipe.AddMessage(bytes))
                {
                    Console.WriteLine("Reader could not send to parser");
                }

            },1,2,10000);

            producerPipe = new BlockingDataflowMessagePipe<string>(mesage =>
            {
                if (!readerPipe.AddMessage("A string"))
                {
                    Console.WriteLine("Producer could not send message to reader");
                }
            },1,1,1000);


            for (int i = 0; i < 3000; i++)
            {
                producerPipe.AddMessage("string");
            }


            Console.ReadKey();

        }

        
    }
}

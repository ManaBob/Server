using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;

namespace TestChatClient
{
   

    class Program
    {

        static void OnSendCB(object _sender, int _slen)
        {
            INetService netSvc = _sender as INetService;
            netSvc.RecvAsync();
        }

        static void OnRecvCB(object _sender, Byte[] _bytes)
        {
            INetService netSvc = _sender as INetService;

            String output = String.Format("Received : {0}", Encoding.UTF8.GetString(_bytes));
            Console.WriteLine(output);
        }

        static void Main(string[] args)
        {

            IPEndPoint serverEp = new IPEndPoint(IPAddress.Loopback, 6670);
            INetService netSvc = new WindowsTCPNetService(serverEp);
            netSvc.OnSend += OnSendCB;
            netSvc.OnRecv += OnRecvCB;
            
            try
            {
                while (true)
                {
                    Console.Write("Client >> ");
                    String input = Console.ReadLine();

                    netSvc.SendAsync(Encoding.UTF8.GetBytes(input), true);
                }
            }
            catch (Exception _exc)
            {
                Console.WriteLine(_exc.Message);
                Console.ReadKey();
                return;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;
using Chat.Portable;

namespace EchoServer
{

    class Program
    {
        static IAccepter            Acc     
            = new WindowsTCPAcceptor(new IPEndPoint(IPAddress.Any, 6670));
        static List<IConnection>    Conns   
            = new List<IConnection>();


        static void OnEchoRecv(object _sender, byte[] _bytes)
        {
            IConnection _conn = _sender as IConnection;
            if (_conn == null)
            {
                return;
            }
            try
            {
                Console.WriteLine(String.Format("Recv : {0}", Encoding.UTF8.GetString(_bytes)));
                _conn.WriteAsync(_bytes, true);
            }
            catch (Exception _exc)
            {
                _conn.Close();
            }
        }

        static void OnEchoSend(object _sender, int _slen)
        {
            IConnection _conn = _sender as IConnection;
            if (_conn == null)
            {
                return;
            }
            try
            {
                Console.WriteLine(String.Format("Sent : {0}", _slen));
                _conn.ReadAsync();
            }
            catch (Exception _exc)
            {
                _conn.Close();
            }
        }

        static void OnClose(object _sender, EventArgs _ev)
        {
            IConnection _conn = _sender as IConnection;
            if (_conn == null)
            {
                return;
            }
            Conns.Remove(_conn);
        }

        static void OnConnect(object _sender, IConnection _conn)
        {
            Conns.Add(_conn);

            EchoSetup(_conn);
            // Start Echo
            _conn.ReadAsync();
            // Accept another
            Acc.AcceptAsync();
        }

        static void EchoSetup(IConnection _conn)
        {
            _conn.OnReceive += OnEchoRecv;
            _conn.OnSend    += OnEchoSend;
            _conn.OnClose   += OnClose;
            _conn.OnDisconnect += (s, e) =>
            {
                Conns.Remove(_conn);
            };
            _conn.OnException += (object s, Exception e) =>
            {
                Console.WriteLine(e.Message);
            };
        }

        static void Main(string[] args)
        {
            Acc.OnConnection += OnConnect;
            Acc.OnConnection += (s, e) =>
            {
                Console.WriteLine("Accepted.");
                Console.Write("Total Connection : ");
                Console.WriteLine(Conns.Count);
            };

            while (true)
            {
                Console.WriteLine(">> ");
                String input = Console.ReadLine();
                String output;

                if (input == "accept")
                {
                    Acc.AcceptAsync();

                    output = String.Format("Accepting...");
                    Console.WriteLine(output);
                }
                if (input == "exit")
                {
                    foreach (var conn in Conns)
                    {
                        try
                        {
                            conn.Close();
                        }
                        catch(Exception _exc)
                        {
                            Console.WriteLine(_exc.Message);
                        }
                    }
                    return;
                }
            }

        }
    }
}

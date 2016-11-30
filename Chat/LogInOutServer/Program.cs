using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;

using Chat.Portable;

namespace LogInOutServer
{

    public class LogInOutTCP : IDisposable
    {
        public IAccepter Acc = null;

        public LogInOutTCP(IPEndPoint _ep)
        {
            Acc = new WindowsTCPAcceptor(_ep);

            Acc.OnConnection = OnConnect;
        }

        public void Accept()
        {
            this.Acc.AcceptAsync();
        }

        void OnConnect(object _sender, IConn _conn)
        {
            // Treat connection as anonymous
            _conn.OnRecv += this.OnRecvAnonymous;
            _conn.OnSend += this.OnSendAnonymous;
            _conn.OnClose += this.OnClose;
            _conn.OnDisconnect += this.OnDisconn;
            _conn.OnException += this.OnExc;

            // Start Handling
            _conn.ReadAsync();
        }

        void OnExc(object _sender, Exception _exc)
        {
            Console.WriteLine(_exc.Message);
        }

        void OnDisconn(object _sender, IConn _conn)
        {
            _conn.Close();
        }


        void OnRecvUser(object _sender, Byte[] _bytes)
        {
            User user = _sender as User;
            if(user == null) { return; }

            // Log the request
            var output = String.Format("Request : {0}", Encoding.UTF8.GetString(_bytes));
            Console.WriteLine(output);

            try{
                // Echo response
                var response = _bytes;
                user.Send(response);
            }
            catch(Exception _exc){
                OnExc(this, _exc);
            }
        }

        void OnSendUser(object _sender, int _slen)
        {
            User user = _sender as User;
            if (user == null) { return; }

            try
            {
                // Log the response
                Console.WriteLine("Response Sent");

                // Recv another request
                user.Recv();
            }
            catch (Exception _exc){
                OnExc(this, _exc);
            }
        }


        void OnRecvAnonymous(object _sender, byte[] _bytes)
        {
            IConn _conn = _sender as IConn;
            if (_conn == null) { return; }

            try
            {
                // Anonymous's request
                var message = Encoding.UTF8.GetString(_bytes);
                Console.WriteLine(String.Format("Recv : {0}", message));

                // Remove old callbacks
                _conn.OnSend -= this.OnSendAnonymous;
                _conn.OnRecv -= this.OnRecvAnonymous;

                // Handle the request
                User promoted = new User(message, _conn);
                promoted.OnSend += OnSendUser;
                promoted.OnRecv += OnRecvUser;

                // Send a response
                Byte[] response = Encoding.UTF8.GetBytes("Login Success");
                promoted.Send(response);
            }
            catch (Exception _exc)
            {
                _conn.Close();
                this.OnExc(this, _exc);
            }
        }
        
        void OnSendAnonymous(object _sender, int _slen)
        {
            IConn _conn = _sender as IConn;
            if (_conn == null) { return; }

            try{
                _conn.ReadAsync();
            }
            catch (Exception _exc){
                _conn.Close();
                OnExc(this, _exc);
            }
        }


        void OnClose(object _sender, EventArgs _ev)
        {
            IConn _conn = _sender as IConn;
            if (_conn == null){ return; }

            _conn.Dispose();
        }

        public void Dispose()
        {
            Acc.Dispose();
            Console.WriteLine("Disposed Acceptor");

            // Dispose current connections...

            // Dispose related resources...
        }

    }

    class Program
    {
        static void Main(string[] args)
        {
            var ep = new IPEndPoint(IPAddress.Any, 6670);


            LogInOutTCP svc = new LogInOutTCP(ep);

            svc.Acc.OnConnection += (s, e) =>
            {
                Console.WriteLine("Accepted.");
                svc.Acc.AcceptAsync();
            };

            while(true)
            {
                svc.Accept();
                if(Console.ReadLine() != "exit")
                {
                    svc.Dispose();
                    return;
                }
            }

        }
    }
}

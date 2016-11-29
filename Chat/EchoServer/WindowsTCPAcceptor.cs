using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;

using Chat.Portable;

namespace EchoServer
{
    /// <summary>
    /// TCP Base Accepter. Nonblocking
    /// </summary>
    public class WindowsTCPAcceptor : IAccepter
    {
        Socket accSock = new Socket(AddressFamily.InterNetwork, 
                                    SocketType.Stream, 
                                    ProtocolType.Tcp);

        EventHandler<IConnection> onConnect;

        public WindowsTCPAcceptor(IPEndPoint _serverEp, int _backlog = 7)
        {
            accSock.Bind(_serverEp);
            accSock.Listen(_backlog);
            accSock.Blocking = false;
        }

        public EventHandler<IConnection> OnConnection
        {
            get { return onConnect;  }
            set { onConnect = value; }
        }

        public void AcceptAsync()
        {
            accSock.BeginAccept(new AsyncCallback(OnAccept), accSock);
        }

        public void OnAccept(IAsyncResult _conn)
        {
            if (_conn == null) { return; }
            Socket aSock = (Socket)_conn.AsyncState;
            Socket cSock = aSock.EndAccept(_conn);

            // Handle Connection...
            IConnection conn = new WindowsTCPConnection(cSock);
            if (OnConnection != null)
            {
                OnConnection.Invoke(this, conn);
            }
            else
            {
                // If can't handle the connection, dispose it.
                conn.Dispose();
            }
        }

        public void Dispose()
        {
            try
            {
                accSock.Close();
            }
            catch (Exception _exc) { }
            finally
            {
                accSock.Dispose();
            }
        }

    }

}

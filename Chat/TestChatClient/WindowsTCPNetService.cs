using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;

namespace TestChatClient
{
    public class WindowsTCPNetService : INetService
    {
        Socket sock = new Socket(AddressFamily.InterNetwork,
                                 SocketType.Stream,
                                 ProtocolType.Tcp);
        Byte[] buffer = new Byte[1024];

        EventHandler<int>       onSendHandler;
        EventHandler<byte[]>    onRecvHandler;
        EventHandler            onDisconnectHandler;
        EventHandler<Exception> onExceptionHandler;

        public WindowsTCPNetService(IPEndPoint _serverEp)
        {
            // Connect directly
            sock.Connect(_serverEp);
        }

        public bool IsAlive
        {
            get
            {
                if (sock.Connected == false)
                {
                    if (onDisconnectHandler != null)
                    {
                        onDisconnectHandler.Invoke(this, null);
                    }
                    return false;
                }
                return true;
            }

        }

        public EventHandler<int>        OnSend
        {
            get{    return onSendHandler;   }
            set{    onSendHandler = value;  }
        }
        public EventHandler<byte[]>     OnRecv
        {
            get { return onRecvHandler; }
            set { onRecvHandler = value; }
        }
        public EventHandler             OnDisconnect
        {
            get { return onDisconnectHandler; }
            set { onDisconnectHandler = value; }
        }
        public EventHandler<Exception>  OnException
        {
            get { return onExceptionHandler; }
            set { onExceptionHandler = value; }
        }


        private void Resize(int _len)
        {
            while (buffer.Length < _len)
            {
                buffer = new Byte[buffer.Length * 2];
            }
        }

        public void SendAsync(byte[] _message, bool _ownership = false)
        {
            // Check for alive
            if (this.IsAlive == false) { return; }

            // If no ownership, copt the contents
            if (_ownership == false)
            {
                Resize(_message.Length);
                Buffer.BlockCopy(_message, 0, buffer, 0, _message.Length);

                _message = this.buffer;
            }
            sock.BeginSend(_message, 0, _message.Length, 0, new AsyncCallback(OnSendAsync), sock);
        }

        private void OnSendAsync(IAsyncResult _ar)
        {
            try
            {
                // Acquire the result
                int slen = sock.EndSend(_ar);
                // Send Error
                if (slen < 0)
                {
                    return;
                }
                // Send Callback
                else if (OnSend != null)
                {
                    OnSend.Invoke(this, slen);
                }
            }
            catch (SocketException _sexc)
            {
                if (this.IsAlive)
                {
                    OnException(this, _sexc);
                }
                else
                {
                    OnException(this, _sexc);
                }
            }
        }


        public void RecvAsync()
        {
            // Check for alive
            if (IsAlive == false) { return; }

            sock.BeginReceive(buffer, 0, buffer.Length, 0, new AsyncCallback(OnRecvAsync), sock);
        }

        private void OnRecvAsync(IAsyncResult _ar)
        {
            try
            {
                // Acquire the result
                int rlen = sock.EndReceive(_ar);
                // Recv Error
                if (rlen < 0)
                {
                    return;
                }

                // Copy the result
                Byte[] recvBuffer = new Byte[rlen];
                Buffer.BlockCopy(buffer, 0, recvBuffer, 0, rlen);

                // Receive Callback
                if (OnRecv != null)
                {
                    OnRecv.Invoke(this, recvBuffer);
                }
            }
            catch (SocketException _sexc)
            {
                if (this.IsAlive)
                {
                    OnException(this, _sexc);
                }
                else
                {
                    OnException(this, _sexc);
                }
            }
        }



        public void Dispose()
        {
            sock.Dispose();
        }



    }
}

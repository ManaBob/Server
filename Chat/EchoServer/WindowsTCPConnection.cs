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
    /// TCP Connection for Windows. Nonblocking. Internal byte buffer
    /// </summary>
    public class WindowsTCPConnection : IConnection
    {
        Socket sock;
        byte[] buffer = new byte[512];

        EventHandler<Byte[]>    onReceive;
        EventHandler<int>       onSend;
        EventHandler            onClose;
        EventHandler            onDisconnect;
        EventHandler<Exception> onExcept;


        public WindowsTCPConnection(Socket _sock)
        {
            if (_sock.ProtocolType != ProtocolType.Tcp)
            {
                throw new ArgumentException();
            }
            this.sock = _sock;
            this.sock.Blocking = false;
        }


        public EventHandler<byte[]>     OnReceive
        {
            get { return onReceive; }
            set { onReceive = value; }
        }
        public EventHandler<int>        OnSend
        {
            get { return onSend; }
            set { onSend = value; }
        }
        public EventHandler             OnClose
        {
            get{    return onClose;     }
            set{    onClose = value;    }
        }
        public EventHandler             OnDisconnect
        {
            get { return onDisconnect; }
            set { onDisconnect = value; }
        }
        public EventHandler<Exception>  OnException
        {
            get { return onExcept; }
            set { onExcept = value; }
        }


        public void WriteAsync(Byte[] _bytes, bool _ownership = false)
        {
            // If not owner of buffer, copy to internal buffer
            if (_ownership == false)
            {
                Resize(_bytes.Length);
                Buffer.BlockCopy(_bytes, 0, buffer, 0, _bytes.Length);
                _bytes = buffer;
            }
            // else use the argument
            sock.BeginSend(_bytes, 0, _bytes.Length, 0, new AsyncCallback(OnWrite), sock);
            
        }

        private void OnWrite(IAsyncResult _ar)
        {
            try
            {
                int slen = sock.EndSend(_ar);
                // Invoke Callback
                if (OnSend != null)
                {
                    OnSend.Invoke(this, slen);
                }
            }
            catch (Exception _exc)
            {
                OnException(this, _exc);
            }

        }


        public void ReadAsync()
        {
            sock.BeginReceive(buffer, 0, buffer.Length, 0, new AsyncCallback(OnRead), sock);
        }

        private void OnRead(IAsyncResult _ar)
        {
            try
            {
                int rlen = sock.EndReceive(_ar);

                // Create Chunk for callback
                Byte[] recvBuf = new Byte[rlen];
                Buffer.BlockCopy(buffer, 0, recvBuf, 0, recvBuf.Length);
                // Invoke Callback
                if (OnReceive != null)
                {
                    OnReceive.Invoke(this, recvBuf);
                }
            }
            catch (Exception _exc)
            {
                OnException(this, _exc);
            }

        }




        public bool IsAlive
        {
            get
            {
                if(sock.Connected == false)
                {
                    OnDisconnect.Invoke(this, null);
                    return false;
                }
                return true;
            }
        }
        

        public void Close()
        {
            this.Dispose();
        }

        public void Dispose()
        {
            if(OnClose != null)
            {
                OnClose.Invoke(this, null);
            }
            sock.Dispose();
        }

        /// <summary>
        /// Expand internal buffer size
        /// </summary>
        private void Resize(int _size)
        {
            while (buffer.Length < _size)
            {
                buffer = new byte[buffer.Length * 2];
            }
        }

    }


}

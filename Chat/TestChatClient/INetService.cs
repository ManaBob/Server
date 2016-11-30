using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;

namespace TestChatClient
{
    interface INetService : IDisposable
    {
        void SendAsync(byte[] _message, bool _ownership = false);
        void RecvAsync();

        bool IsAlive    { get;}

        EventHandler<int>       OnSend { get; set; }
        EventHandler<byte[]>    OnRecv { get; set; }
        EventHandler            OnDisconnect { get; set; }
        EventHandler<Exception> OnException { get; set; }
    }
}

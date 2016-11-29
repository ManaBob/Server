using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Chat.Portable
{
    public interface IConnection : IDisposable
    {
        void WriteAsync(Byte[] _bytes, bool _ownership);
        void ReadAsync();
        void Close();

        bool IsAlive        { get; }

        EventHandler<Byte[]>    OnReceive    { get; set; }
        EventHandler<int>       OnSend       { get; set; }
        EventHandler            OnClose      { get; set; }
        EventHandler            OnDisconnect { get; set; }
        EventHandler<Exception> OnException  { get; set; }

    }

}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Chat.Portable
{
    public interface IConn : IDisposable
    {
        void WriteAsync(Byte[] _bytes, bool _ownership);
        void ReadAsync();
        void Close();

        bool IsAlive        { get; }

        EventHandler<Byte[]>    OnRecv          { get; set; }
        EventHandler<int>       OnSend          { get; set; }
        EventHandler            OnClose         { get; set; }
        EventHandler<IConn>     OnDisconnect    { get; set; }
        EventHandler<Exception> OnException     { get; set; }

    }

}

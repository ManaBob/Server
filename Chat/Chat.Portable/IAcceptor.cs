using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;

namespace Chat.Portable
{

    public interface IAccepter : IDisposable
    {
        EventHandler<IConnection> OnConnection { get; set; }
        void AcceptAsync();

    }

}

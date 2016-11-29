using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Chat.Portable;

namespace LogInOutServer
{
    public class User
    {
        IConn conn = null;

        public int    ID    { get; set; }
        public String Name  { get; set; }

        public EventHandler<int>    OnSend;
        public EventHandler<Byte[]> OnRecv;

        public User(String _name, IConn _conn)
        {
            if(_name == null || _conn == null)
            {
                throw new ArgumentNullException();
            }
            else if(_name.Length == 0)
            {
                throw new ArgumentException();
            }

            this.conn   = _conn;
            this.Name   = _name;
            this.ID     = _name.GetHashCode();

            this.conn.OnSend += this.OnUserSend;
            this.conn.OnRecv += this.OnUserRecv;
        }

        public void Send(Byte[] _buffer)
        {
            conn.WriteAsync(_buffer, true);
        }

        public void Recv()
        {
            conn.ReadAsync();
        }

        private void OnUserSend(object _conn, int _slen)
        {
            if(OnSend != null){
                OnSend.Invoke(this, _slen);
            }
        }

        private void OnUserRecv(object _conn, Byte[] _bytes)
        {
            if(OnRecv != null){
                OnRecv.Invoke(this, _bytes);
            }
        }




    }
}

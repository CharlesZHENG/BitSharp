using BitSharp.Core;
using BitSharp.Core.JsonRpc;

namespace BitSharp.Node
{
    public class NodeRpcServer : CoreRpcServer
    {
        private readonly CoreDaemon coreDaemon;

        public NodeRpcServer(CoreDaemon coreDaemon, int port)
            : base(coreDaemon, port)
        {
            this.coreDaemon = coreDaemon;
        }
    }
}

using BitSharp.Core;
using BitSharp.Core.JsonRpc;
using NLog;

namespace BitSharp.Node
{
    public class NodeRpcServer : CoreRpcServer
    {
        private readonly CoreDaemon coreDaemon;

        public NodeRpcServer(Logger logger, CoreDaemon coreDaemon)
            : base(logger, coreDaemon)
        {
            this.coreDaemon = coreDaemon;
        }
    }
}

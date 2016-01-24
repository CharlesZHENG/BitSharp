using BitSharp.Network.Domain;
using System.Linq;
using System.Net;

namespace BitSharp.Network.ExtensionMethods
{
    public static class ExtensionMethods
    {
        public static IPEndPoint ToIPEndPoint(this NetworkAddress networkAddress)
        {
            var address = new IPAddress(networkAddress.IPv6Address.ToArray());
            if (address.IsIPv4MappedToIPv6)
                address = new IPAddress(networkAddress.IPv6Address.Skip(12).ToArray());

            return new IPEndPoint(address, networkAddress.Port);
        }
    }
}

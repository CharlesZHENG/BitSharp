
namespace BitSharp.Core.Script
{
    public class PayToPublicKeyBuilder
    {
        public byte[] CreateOutput(byte[] publicKey)
        {
            using (var outputScript = new ScriptBuilder())
            {
                outputScript.WritePushData(publicKey);
                outputScript.WriteOp(ScriptOp.OP_CHECKSIG);

                return outputScript.GetScript();
            }
        }
    }
}
